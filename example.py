from maga import Maga, get_metadata
import asyncio
import logging
import os
import re
from multiprocessing import Process
from screenshot_system.orchestrator import create_screenshots_from_stream
from screenshot_system.downloader import Downloader
from screenshot_system.io_adapter import TorrentFileIO

logging.basicConfig(level=logging.INFO)

# --- Start of Classification System ---

CLASSIFICATION_RULES = {
    'japanese_av': {
        'type': 'hybrid',
        'regex': r'\b[A-Z]+-\d+\b',
        'keywords': ['jav', 'fc2']
    },
    'chinese_homemade': {
        'type': 'keywords',
        'words': [
            '自拍', '探花', '寻花', '原创', '泄密', '流出', '调教', '露出', '口交', '啪啪啪', '做爱', '操', '插', '射',
            '淫', '骚', '逼', '穴', '屌', '后庭', '潮喷', '自慰', '群P', '3P', '乱伦', '奸', '学生', '少妇', '人妻',
            '女神', '嫩妹', '小姐姐', '美女', '学妹', '网红', '名媛', '外围', '舞姬', '老师', '夫妻', '情侣', '国产',
            '國產', '91', '精东', '麻豆', '天美', '海角', '推特'
        ]
    }
}

def classify_torrent(name):
    name_lower = name.lower()
    for category, rule in CLASSIFICATION_RULES.items():
        rule_type = rule.get('type')
        if rule_type == 'hybrid':
            if re.search(rule['regex'], name):
                return category
            for word in rule['keywords']:
                if word in name_lower:
                    return category
        elif rule_type == 'keywords':
            for word in rule['words']:
                if word.lower() in name_lower:
                    return category
    return None

# --- End of Classification System ---

def run_screenshot_task(infohash: str, target_path: str):
    """
    This function runs in a separate process.
    It handles the entire lifecycle of downloading and screenshotting for one torrent.
    """
    downloader = Downloader()
    try:
        handle = downloader.get_torrent_handle(infohash)
        if not handle:
            logging.error(f"TASK {infohash}: Could not get handle for '{target_path}'.")
            return

        torrent_file = handle.torrent_file()
        if not torrent_file:
            logging.error(f"TASK {infohash}: Could not get torrent_file from handle for '{target_path}'.")
            return

        files = torrent_file.files()

        # Find the file index in this context by matching the target path
        target_file_index = -1
        for i in range(files.num_files()):
            if files.file_path(i) == target_path:
                target_file_index = i
                break

        if target_file_index == -1:
            logging.error(f"TASK {infohash}: Could not find file path '{target_path}' in torrent.")
            # DEBUG: List all available files if not found
            print(f"Orchestrator: DEBUG: Listing all available files in torrent for target '{target_path}':")
            for i in range(files.num_files()):
                print(f"  - File {i}: {files.file_path(i)}")
            return

        file_size = files.file_size(target_file_index)

        logging.info(f"TASK {infohash}: Creating stream for file '{target_path}' (index {target_file_index}, size {file_size})")

        io_adapter = TorrentFileIO(downloader, infohash, target_file_index, file_size)

        # The orchestrator is now generic and just processes the stream
        create_screenshots_from_stream(io_adapter)

    except Exception as e:
        logging.error(f"TASK {infohash}: An unexpected error occurred: {e}", exc_info=True)
    finally:
        downloader.close_session()
        logging.info(f"TASK {infohash}: Process finished.")


class Crawler(Maga):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_infohashes = set()

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        if infohash in self.processed_infohashes:
            return

        loop = asyncio.get_event_loop()
        try:
            metadata = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)
        except Exception:
            # Malformed metadata can cause crashes in bdecode, ignore them
            return

        if not metadata:
            return

        self.processed_infohashes.add(infohash)

        torrent_name_bytes = metadata.get(b'name')
        if not torrent_name_bytes:
            return

        torrent_name_str = torrent_name_bytes.decode('utf-8', 'ignore')

        category = classify_torrent(torrent_name_str)
        if not category:
            return

        logging.info(f"Classifier: Found target torrent '{torrent_name_str}' in category '{category}'")

        # Find the path of the largest .mp4 file
        target_path_parts = None
        largest_size = 0

        if b'files' in metadata:  # Multi-file torrent
            for f in metadata.get(b'files', []):
                path_parts_bytes = f.get(b'path', [])
                if not path_parts_bytes:
                    continue

                filename = path_parts_bytes[-1].decode('utf-8', 'ignore')
                if filename.lower().endswith('.mp4'):
                    file_size = f.get(b'length', 0)
                    if file_size > largest_size:
                        largest_size = file_size
                        target_path_parts = [p.decode('utf-8', 'ignore') for p in path_parts_bytes]
        else:  # Single-file torrent
            if torrent_name_str.lower().endswith('.mp4'):
                target_path_parts = [torrent_name_str]

        if target_path_parts:
            # Construct the full path. For multi-file torrents, the torrent name
            # is typically the root directory.
            if b'files' in metadata:
                full_path_parts = [torrent_name_str] + target_path_parts
                target_path = "/".join(full_path_parts)
            else:
                target_path = torrent_name_str

            logging.info(f"Handing off to screenshot process: infohash={infohash}, target_path={target_path}")
            # Run the screenshot task in a separate process to isolate crashes
            p = Process(target=run_screenshot_task, args=(infohash, target_path))
            p.daemon = True
            p.start()
        else:
            logging.info(f"No .mp4 file found in torrent {infohash}")


if __name__ == "__main__":
    crawler = Crawler()
    crawler.run(6881)
