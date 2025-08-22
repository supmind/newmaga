from maga import Maga, get_metadata
import asyncio
import logging
logging.basicConfig(level=logging.INFO)


class Crawler(Maga):
    async def handle_announce_peer(self, infohash, addr, peer_addr):
        loop = asyncio.get_event_loop()
        metadata = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)
        if metadata:
            logging.info("Successfully downloaded metadata for infohash: %s", infohash)
            logging.info("Metadata: %s", metadata)

crawler = Crawler()
crawler.run(6881)
