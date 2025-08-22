from maga import Maga, get_metadata
import asyncio
import logging
import os
import re
from screenshot_system.orchestrator import create_screenshots_for_torrent

logging.basicConfig(level=logging.INFO)

# --- Start of Classification System ---

CLASSIFICATION_RULES = {
    'japanese_av': {
        'type': 'hybrid',
        # Stricter, case-sensitive regex for standard codes like 'ABC-123'
        'regex': r'\b[A-Z]+-\d+\b',
        'keywords': ['jav', 'fc2'] # Keywords for other formats, checked case-insensitively
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
            # Regex for this rule is case-sensitive
            if re.search(rule['regex'], name):
                return category
            # Keywords are case-insensitive
            for word in rule['keywords']:
                if word in name_lower:
                    return category
        elif rule_type == 'keywords':
            for word in rule['words']:
                if word.lower() in name_lower:
                    return category
    return None

# --- End of Classification System ---


class Crawler(Maga):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_infohashes = set()

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        # Avoid processing the same torrent multiple times
        if infohash in self.processed_infohashes:
            return

        # We need the metadata to classify and get file lists
        # The original get_metadata is sufficient for this initial step
        loop = asyncio.get_event_loop()
        metadata = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)
        if not metadata:
            return

        self.processed_infohashes.add(infohash)

        torrent_name_bytes = metadata.get(b'name')
        if not torrent_name_bytes:
            return

        torrent_name_str = torrent_name_bytes.decode('utf-8', 'ignore')

        # 1. Classify the torrent
        category = classify_torrent(torrent_name_str)
        if not category:
            return

        logging.info(f"Classifier: Found target torrent '{torrent_name_str}' in category '{category}'")

        # 2. Find the target video file (largest .mp4)
        target_file_index = -1
        largest_size = 0

        if b'files' in metadata: # Multi-file
            for i, f in enumerate(metadata[b'files']):
                path_parts = f.get(b'path', [])
                if path_parts:
                    filename = path_parts[-1].decode('utf-8', 'ignore')
                    if filename.lower().endswith('.mp4'):
                        if f.get(b'length', 0) > largest_size:
                            largest_size = f.get(b'length', 0)
                            target_file_index = i
        else: # Single-file
             if torrent_name_str.lower().endswith('.mp4'):
                 target_file_index = 0

        # 3. If we found a video file, start the screenshot process in a separate thread
        if target_file_index != -1:
            logging.info(f"Handing off to screenshot orchestrator: {infohash}, file index {target_file_index}")
            loop.run_in_executor(
                None, # Use the default thread pool executor
                create_screenshots_for_torrent,
                infohash,
                target_file_index
            )
        else:
            logging.info(f"No .mp4 file found in torrent {infohash}")


crawler = Crawler()
crawler.run(6881)
