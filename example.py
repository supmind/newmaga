from maga import Maga, get_metadata
import asyncio
import logging
import os
import re

logging.basicConfig(level=logging.INFO)

# --- Start of Classification System ---

CLASSIFICATION_RULES = {
    'japanese_av': {
        'type': 'hybrid',
        'regex': r'\b[A-Z0-9]{2,5}-?\d{2,5}\b',
        'keywords': ['jav', 'jc']
    },
    'chinese_homemade': {
        'type': 'keywords',
        'words': [
            # Explicit terms
            '自拍', '探花', '寻花', '原创', '泄密', '流出', '调教', '露出', '口交',
            '啪啪啪', '做爱', '操', '插', '射', '淫', '骚', '逼', '穴', '屌', '后庭',
            '潮喷', '自慰', '群P', '3P', '乱伦', '奸',
            # Identity
            '学生', '少妇', '人妻', '女神', '嫩妹', '小姐姐', '美女', '学妹', '网红',
            '名媛', '外围', '舞姬', '老师', '夫妻', '情侣',
            # Origin/Platform
            '国产', '國產', '91', '精东', '麻豆', '天美', '海角', '推特'
        ]
    }
}

def classify_torrent(name):
    """
    Classifies a torrent based on its name.
    Returns the category name as a string, or None if no category matches.
    """
    name_lower = name.lower()
    for category, rule in CLASSIFICATION_RULES.items():
        rule_type = rule.get('type')
        if rule_type == 'regex':
            if re.search(rule.get('regex'), name, re.IGNORECASE):
                return category
        elif rule_type == 'keywords':
            for word in rule['words']:
                if word.lower() in name_lower:
                    return category
        elif rule_type == 'hybrid':
            if re.search(rule.get('regex'), name, re.IGNORECASE):
                return category
            for word in rule['keywords']:
                if word in name_lower:
                    return category
    return None

# --- End of Classification System ---


class Crawler(Maga):
    async def handle_announce_peer(self, infohash, addr, peer_addr):
        loop = asyncio.get_event_loop()
        metadata = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)
        if not metadata:
            return

        torrent_name_bytes = metadata.get(b'name')
        if not torrent_name_bytes:
            return

        torrent_name_str = torrent_name_bytes.decode('utf-8', 'ignore')

        # 1. Classify the torrent
        category = classify_torrent(torrent_name_str)
        if not category:
            return

        # 2. Check for .mp4 files
        has_mp4 = False
        if b'files' in metadata:  # Multi-file
            for f in metadata[b'files']:
                path_parts = f.get(b'path')
                if path_parts:
                    filename = path_parts[-1].decode('utf-8', 'ignore')
                    if filename.lower().endswith('.mp4'):
                        has_mp4 = True
                        break
        else:  # Single-file
            if torrent_name_str.lower().endswith('.mp4'):
                has_mp4 = True

        if not has_mp4:
            return

        # 3. If all filters pass, log the information
        logging.info("Successfully downloaded metadata for infohash: %s", infohash)
        logging.info(f"Torrent Name: {torrent_name_str}")
        logging.info(f"Category: {category}")

        if b'files' in metadata:
            # Multi-file torrent
            for f in metadata[b'files']:
                file_path = os.path.join(*[path_part.decode('utf-8', 'ignore') for path_part in f[b'path']])
                file_size = f.get(b'length', 'N/A')
                logging.info(f"  - File: {file_path}, Size: {file_size} bytes")
        else:
            # Single-file torrent
            file_size = metadata.get(b'length', 'N/A')
            logging.info(f"File: {torrent_name_str}, Size: {file_size} bytes")

crawler = Crawler()
crawler.run(6881)
