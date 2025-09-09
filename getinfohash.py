import asyncio
import binascii
import logging
import signal
from maga.crawler import Maga

# 配置基本的日志记录以查看输出
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class InfohashCrawler(Maga):
    """
    一个简单的爬虫，只打印来自 announce_peer 消息的 infohash。
    """
    async def handle_announce_peer(self, infohash, addr, peer_addr):
        """
        当收到 'announce_peer' 消息时，此处理程序被调用。
        """
        infohash_hex = binascii.hexlify(infohash).decode()
        logging.info(f"从 {addr} 发现 infohash: {infohash_hex}")

    async def handle_get_peers(self, infohash, addr):
        """
        当收到 'get_peers' 消息时，此处理程序被调用。
        记录这些也很有用，因为它们是infohash的主要来源。
        """
        infohash_hex = binascii.hexlify(infohash).decode()
        logging.info(f"从 {addr} 收到 get_peers 请求，infohash 为: {infohash_hex}")


async def main():
    loop = asyncio.get_running_loop()
    
    # 创建我们的简单爬虫实例
    crawler = InfohashCrawler()
    
    # 在一个随机可用端口上运行爬虫
    await crawler.run(port=6881)
    logging.info("爬虫正在运行。按 Ctrl+C 停止。")

    # 设置信号处理程序以实现优雅关闭
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    
    await stop

    logging.info("正在停止爬虫...")
    crawler.stop()
    logging.info("爬虫已停止。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
