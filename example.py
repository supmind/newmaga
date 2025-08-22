from maga import Maga, get_metadata
import asyncio
import logging
import os
import re

logging.basicConfig(level=logging.INFO)


class Crawler(Maga):
    async def handle_announce_peer(self, infohash, addr, peer_addr):
        loop = asyncio.get_event_loop()
        metadata = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)
        if not metadata:
            return

        # Requirement 1: Torrent name must contain Chinese or English characters.
        torrent_name_bytes = metadata.get(b'name')
        if not torrent_name_bytes:
            return
        torrent_name_str = torrent_name_bytes.decode('utf-8', 'ignore')
        if not re.search(r'[\u4e00-\u9fa5a-zA-Z]', torrent_name_str):
            return

        # Requirement 2: Must contain an .mp4 file.
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

        # If both conditions are met, log the info
        logging.info("Successfully downloaded metadata for infohash: %s", infohash)

        if b'files' in metadata:
            # Multi-file torrent
            logging.info(f"Torrent Name: {torrent_name_str}")
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
