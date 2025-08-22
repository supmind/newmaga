from maga import Maga, get_metadata
import asyncio
import logging
import os

logging.basicConfig(level=logging.INFO)


class Crawler(Maga):
    async def handle_announce_peer(self, infohash, addr, peer_addr):
        loop = asyncio.get_event_loop()
        metadata = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)
        if metadata:
            logging.info("Successfully downloaded metadata for infohash: %s", infohash)

            if b'files' in metadata:
                # Multi-file torrent
                torrent_name = metadata[b'name'].decode('utf-8', 'ignore')
                logging.info(f"Torrent Name: {torrent_name}")
                for f in metadata[b'files']:
                    file_path = os.path.join(*[path_part.decode('utf-8', 'ignore') for path_part in f[b'path']])
                    file_size = f[b'length']
                    logging.info(f"  - File: {file_path}, Size: {file_size} bytes")
            else:
                # Single-file torrent
                file_name = metadata[b'name'].decode('utf-8', 'ignore')
                file_size = metadata[b'length']
                logging.info(f"File: {file_name}, Size: {file_size} bytes")

crawler = Crawler()
crawler.run(6881)
