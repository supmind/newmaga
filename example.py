from maga import Maga, get_metadata
import asyncio
import logging
import os
import re
import time
import threading
from multiprocessing import Process, Manager
from screenshot_system.orchestrator import create_screenshots_from_stream
from screenshot_system.downloader import Downloader
from screenshot_system.io_adapter import TorrentFileIO

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

def run_screenshot_task(infohash: str, target_file_index: int, file_size: int, queue):
    """
    This function runs in a separate process.
    It handles the entire lifecycle of downloading and screenshotting for one torrent.
    """
    downloader = Downloader()
    try:
        handle = downloader.get_torrent_handle(infohash)
        if not handle:
            return

        # Pre-buffer the first 2MB of the file to ensure PyAV has enough data
        pre_buffer_size = 2 * 1024 * 1024
        downloader.download_byte_range(infohash, target_file_index, 0, pre_buffer_size)

        io_adapter = TorrentFileIO(downloader, infohash, target_file_index, file_size)

        create_screenshots_from_stream(io_adapter, infohash, queue, num_screenshots=20)

    except Exception:
        # Errors are handled silently in the child process
        pass
    finally:
        downloader.close_session()

def statistics_worker(queue):
    """
    A worker that runs in a thread to periodically print stats.
    """
    total_screenshots = 0
    while True:
        time.sleep(60)

        period_screenshots = 0
        while not queue.empty():
            try:
                queue.get_nowait()
                period_screenshots += 1
            except queue.Empty:
                break

        total_screenshots += period_screenshots

        print(f"[STATS] Last 60s: {period_screenshots} screenshots. Total: {total_screenshots} screenshots.")


class Crawler(Maga):
    def __init__(self, queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_infohashes = set()
        self.queue = queue

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        if infohash in self.processed_infohashes:
            return

        loop = asyncio.get_event_loop()
        try:
            metadata = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)
        except Exception:
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

        target_file_index = -1
        largest_size = 0

        files_metadata = metadata.get(b'files')
        if files_metadata:  # Multi-file torrent
            for i, f in enumerate(files_metadata):
                path_parts_bytes = f.get(b'path', [])
                if not path_parts_bytes:
                    continue

                filename = path_parts_bytes[-1].decode('utf-8', 'ignore')
                if filename.lower().endswith('.mp4'):
                    file_size = f.get(b'length', 0)
                    if file_size > largest_size:
                        largest_size = file_size
                        target_file_index = i
        else:  # Single-file torrent
            if torrent_name_str.lower().endswith('.mp4'):
                largest_size = metadata.get(b'length', 0)
                target_file_index = 0

        if target_file_index != -1:
            p = Process(target=run_screenshot_task, args=(infohash, target_file_index, largest_size, self.queue))
            p.daemon = True
            p.start()


if __name__ == "__main__":
    # Set up logging to only show INFO for the main process, not for maga library
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('maga').setLevel(logging.WARNING)

    with Manager() as manager:
        screenshot_queue = manager.Queue()

        stats_thread = threading.Thread(target=statistics_worker, args=(screenshot_queue,), daemon=True)
        stats_thread.start()

        crawler = Crawler(queue=screenshot_queue)
        crawler.run(6881)
