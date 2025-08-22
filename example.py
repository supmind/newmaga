from maga import Maga, get_metadata
import asyncio
import logging
import os
import re

logging.basicConfig(level=logging.INFO)

# --- Start of Classification System ---

CLASSIFICATION_RULES = {
    'japanese_av': {
        'type': 'regex',
        'pattern': r'\b[A-Z]{2,5}-?\d{2,5}\b'
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
    for category, rule in CLASSIFICATION_RULES.items():
        if rule['type'] == 'regex':
            if re.search(rule['pattern'], name, re.IGNORECASE):
                return category
        elif rule['type'] == 'keywords':
            for word in rule['words']:
                if word in name:
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

        # Classify the torrent
        category = classify_torrent(torrent_name_str)

        # Only log torrents that are classified
        if not category:
            return

        # Log the information
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
