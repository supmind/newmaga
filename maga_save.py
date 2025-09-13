import asyncio
import binascii
import logging
import signal
import os
import argparse
import aiohttp
import redis.asyncio as redis

import config
from maga.crawler import Maga
from maga.downloader import get_metadata
from maga.utils import proper_infohash, format_bytes
from fastbencode import bencode

# -- 中文注释 --
# 配置日志记录，用于查看爬虫和脚本的输出
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# 定义元数据存储目录
DOWNLOAD_DIR = "downloads"


async def submit_screenshot_task(infohash, torrent_path):
    """
    Submits a task to the screenshot service.
    """
    url = 'http://47.79.230.210:8000/tasks/'
    api_key = 'a_very_secret_and_complex_key_for_dev'
    headers = {
        'accept': 'application/json',
        'X-API-Key': api_key
    }

    try:
        with open(torrent_path, 'rb') as torrent_file:
            data = aiohttp.FormData()
            data.add_field('infohash', infohash)
            data.add_field('torrent_file',
                           torrent_file,
                           filename=os.path.basename(torrent_path),
                           content_type='application/x-bittorrent')

            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, data=data) as response:
                    if response.status == 200:
                        log.info(f"Successfully submitted screenshot task for {infohash}.")
                    else:
                        log.error(f"Failed to submit screenshot task for {infohash}. Status: {response.status}, Response: {await response.text()}")

    except FileNotFoundError:
        log.error(f"Could not find torrent file for submission: {torrent_path}")
        return
    except aiohttp.ClientError as e:
        log.error(f"An error occurred while submitting task for {infohash}: {e}")


async def metadata_downloader(task_queue, redis_client):
    """
    元数据下载器（消费者/工作者）。
    它从任务队列中获取任务，并下载元数据。
    """
    # 确保下载目录存在
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    while True:
        try:
            infohash, peer_addr = await task_queue.get()
            infohash_hex = proper_infohash(infohash)

            # 异步下载元数据
            # 我们从发起 announce_peer 请求的节点下载
            loop = asyncio.get_running_loop()
            info = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop, timeout=10)

            if info:
                # 下载成功后，将 infohash 添加到已处理集合
                await redis_client.sadd(config.REDIS_PROCESSED_SET, infohash_hex)

                # 从元数据中提取信息
                name = info.get(b'name', b'Unknown').decode(errors='ignore')

                # -- 中文注释 --
                # 将下载的元数据（info字典）编码为bencode格式，并保存为 .torrent 文件
                # 这是制作 .torrent 文件的标准方式
                # The torrent file must be a dictionary containing the 'info' key.
                torrent_data = bencode({b'info': info})
                file_path = os.path.join(DOWNLOAD_DIR, f"{infohash_hex}.torrent")
                with open(file_path, "wb") as f:
                    f.write(torrent_data)

                log.info(f"成功下载并保存元数据: Name: {name}, Infohash: {infohash_hex}")

                # -- BEGIN: Screenshot submission logic --
                has_mp4 = False
                if b'files' in info:
                    for file_info in info[b'files']:
                        path_parts = file_info.get(b'path', [])
                        if path_parts and path_parts[-1].decode(errors='ignore').lower().endswith('.mp4'):
                            has_mp4 = True
                            break
                elif b'name' in info:
                    if info[b'name'].decode(errors='ignore').lower().endswith('.mp4'):
                        has_mp4 = True

                if has_mp4:
                    log.info(f"MP4 file found in {infohash_hex}. Submitting for screenshot.")
                    # Using create_task to avoid blocking the downloader worker
                    loop.create_task(submit_screenshot_task(infohash_hex, file_path))
                # -- END: Screenshot submission logic --

        except asyncio.CancelledError:
            # 如果任务被取消，优雅地退出循环
            break
        except Exception:
            # 记录其他异常，但不要让工作者崩溃
            log.exception(f"处理 infohash 时出错: {infohash_hex}")
        finally:
            # 确保任务被标记为完成，并从排队集合中移除
            # 这样即使失败了，将来也有机会重试
            if 'infohash_hex' in locals():
                await redis_client.srem(config.REDIS_QUEUED_SET, infohash_hex)
            task_queue.task_done()


class SimpleCrawler(Maga):
    """
    DHT 爬虫（生产者）。
    它负责发现 infohash 并将它们放入任务队列。
    """
    def __init__(self, task_queue, redis_client, loop=None):
        super().__init__(loop=loop)
        self.task_queue = task_queue
        self.redis_client = redis_client

    async def handle_get_peers(self, infohash, addr):
        # This crawler is only interested in announce_peer messages
        pass

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        """
        这个处理器在收到 `announce_peer` 消息时被调用。
        它将发现的任务放入队列，供工作者处理。
        """
        infohash_hex = proper_infohash(infohash)

        # 使用 SISMEMBER 一次性检查两个集合
        # [1, 0] -> ismember of processed, not member of queued
        # [0, 1] -> not member of processed, ismember of queued
        # [0, 0] -> not member of either
        is_processed, is_queued = await self.redis_client.smismember(
            [config.REDIS_PROCESSED_SET, config.REDIS_QUEUED_SET],
            [infohash_hex, infohash_hex]
        )

        if is_processed or is_queued:
            return

        # 先添加到排队集合，再放入任务队列
        await self.redis_client.sadd(config.REDIS_QUEUED_SET, infohash_hex)
        try:
            self.task_queue.put_nowait((infohash, peer_addr))
        except asyncio.QueueFull:
            # 如果队列已满，从排队集合中移除，以便稍后有空间时可以重新排队
            await self.redis_client.srem(config.REDIS_QUEUED_SET, infohash_hex)


async def print_stats(crawler, task_queue, redis_client):
    """
    一个定期任务，用于打印爬虫和任务队列的统计信息。
    """
    while True:
        await asyncio.sleep(30)
        queued_count = await redis_client.scard(config.REDIS_QUEUED_SET)
        processed_count = await redis_client.scard(config.REDIS_PROCESSED_SET)
        log.info(
            f"[统计] DHT 节点: {sum(len(b) for b in crawler.k_buckets)} | "
            f"队列大小: {task_queue.qsize()}/{task_queue.maxsize} | "
            f"已排队哈希 (Redis): {queued_count} | "
            f"已处理哈希 (Redis): {processed_count}"
        )


async def main(args):
    """
    基于生产者-消费者模型的爬虫主入口点。
    """
    log.info("启动 DHT 爬虫 (生产者-消费者模型)...")
    loop = asyncio.get_running_loop()

    # 创建 Redis 客户端
    try:
        redis_client = redis.from_url(
            f"redis://{args.redis_host}:{args.redis_port}/{args.redis_db}",
            decode_responses=True
        )
        await redis_client.ping()
        log.info(f"成功连接到 Redis at {args.redis_host}:{args.redis_port}")
    except (redis.exceptions.ConnectionError, ConnectionRefusedError) as e:
        log.error(f"无法连接到 Redis: {e}")
        return

    # 一个有界队列，用作生产者和消费者之间的缓冲区
    task_queue = asyncio.Queue(maxsize=args.queue_size)

    # 爬虫作为生产者
    crawler = SimpleCrawler(
        task_queue=task_queue,
        redis_client=redis_client,
        loop=loop
    )

    # 下载工作者的数量决定了下载的并发度
    workers = [
        loop.create_task(metadata_downloader(task_queue, redis_client))
        for _ in range(args.workers)
    ]

    # 在指定端口上运行爬虫
    await crawler.run(port=args.port)
    log.info(f"爬虫正在监听端口 {crawler.transport.get_extra_info('sockname')[1]}")

    # 启动定期的统计信息打印任务
    stats_task = loop.create_task(print_stats(crawler, task_queue, redis_client))

    log.info(f"{args.workers} 个下载工作者已启动。按 Ctrl+C 停止。")

    # 处理优雅关闭
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    log.info("正在关闭...")
    # 关闭 Redis 连接
    await redis_client.close()
    stats_task.cancel()
    for worker in workers:
        worker.cancel()
    crawler.stop()
    # 等待所有工作者和爬虫任务完成
    await asyncio.gather(*workers, return_exceptions=True)
    await task_queue.join()
    log.info("爬虫已优雅地关闭。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="一个简单而强大的 DHT 爬虫，用于下载种子元数据。")
    parser.add_argument("--port", type=int, default=config.DEFAULT_PORT, help="DHT 监听端口。")
    parser.add_argument("--workers", type=int, default=200, help="并发元数据下载工作者的数量。")
    parser.add_argument("--queue-size", type=int, default=2000, help="任务队列的最大大小。")
    # Redis arguments
    parser.add_argument("--redis-host", type=str, default=config.REDIS_HOST, help="Redis 服务器主机。")
    parser.add_argument("--redis-port", type=int, default=config.REDIS_PORT, help="Redis 服务器端口。")
    parser.add_argument("--redis-db", type=int, default=config.REDIS_DB, help="Redis 数据库编号。")
    args = parser.parse_args()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        log.info("爬虫被用户停止。")
