from maga import Maga

import logging
logging.basicConfig(level=logging.INFO)


class Crawler(Maga):
    async def handle_announce_peer(self, infohash, addr, peer_addr):
        logging.info(infohash)

crawler = Crawler()
crawler.run(6881)
