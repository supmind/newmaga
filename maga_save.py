import asyncio
import binascii
import logging
import signal
import os
import argparse
import aiohttp
import aiofiles
import redis.asyncio as redis

import config
from maga.crawler import Maga
from maga.downloader import get_metadata
from maga.es_handler import ESHandler
from maga.r2_handler import R2Handler
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


async def metadata_downloader(task_queue, redis_client, es_handler, r2_handler):
    """
    元数据下载器（消费者/工作者）。
    它从任务队列中获取任务，下载元数据，然后将其上传到外部服务。
    """
    while True:
        infohash, peer_addr = await task_queue.get()
        infohash_hex = None
        locked = False
        try:
            infohash_hex = proper_infohash(infohash)

            # Atomically check and claim the infohash. If sadd returns 0, it means
            # another worker is already handling it, so we skip.
            if await redis_client.sadd(config.REDIS_QUEUED_SET, infohash_hex) == 0:
                continue
            locked = True

            # Asynchronously download metadata
            loop = asyncio.get_running_loop()
            info = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop, timeout=10)

            if info:
                # This is the new transactional logic block.
                # We only mark as "processed" in Redis if both ES and R2 uploads succeed.
                try:
                    # 1. Prepare data
                    name = info.get(b'name', b'Unknown').decode('utf-8', 'ignore')
                    files_list = info.get(b'files')
                    total_size = sum(f.get(b'length', 0) for f in files_list) if files_list else info.get(b'length', 0)
                    torrent_data = bencode({b'info': info})

                    # 2. Perform uploads
                    await es_handler.upload_document(
                        infohash=infohash_hex,
                        name=name,
                        files_list=files_list,
                        total_size=total_size
                    )
                    await r2_handler.upload_torrent(
                        torrent_data=torrent_data,
                        infohash_hex=infohash_hex
                    )

                    # 3. If BOTH succeed, mark as processed
                    await redis_client.sadd(config.REDIS_PROCESSED_SET, infohash_hex)
                    log.info(f"Successfully processed and uploaded: {infohash_hex} - {name}")

                except Exception as e:
                    # If any upload fails, log the error and do not mark as processed.
                    # The `finally` block will still clean up the queued set.
                    log.error(f"Failed to complete ES/R2 upload for {infohash_hex}: {e}")

        except asyncio.CancelledError:
            break
        except Exception:
            log.exception(f"An error occurred in the metadata downloader for infohash: {infohash_hex}")
        finally:
            # Always mark the task as done in the asyncio queue.
            task_queue.task_done()
            # If we locked this hash for processing, we must unlock it from the Redis queued set.
            if locked and infohash_hex:
                await redis_client.srem(config.REDIS_QUEUED_SET, infohash_hex)


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

        # De-duplication is now handled by the consumer to avoid race conditions.
        # We only do a preliminary check against the processed set to avoid
        # re-queueing torrents that are already done.
        if await self.redis_client.sismember(config.REDIS_PROCESSED_SET, infohash_hex):
            return

        try:
            self.task_queue.put_nowait((infohash, peer_addr))
        except asyncio.QueueFull:
            # If the queue is full, we simply drop the task.
            pass


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

    # Create clients for external services
    try:
        redis_client = redis.from_url(
            f"redis://{args.redis_host}:{args.redis_port}/{args.redis_db}",
            decode_responses=True
        )
        await redis_client.ping()
        log.info(f"成功连接到 Redis at {args.redis_host}:{args.redis_port}")

        es_handler = ESHandler(
            hosts=config.ES_HOSTS,
            username=config.ES_USERNAME,
            password=config.ES_PASSWORD
        )
        await es_handler.init_index()

        r2_handler = R2Handler(
            endpoint_url=config.R2_ENDPOINT_URL,
            access_key_id=config.R2_ACCESS_KEY_ID,
            secret_access_key=config.R2_SECRET_ACCESS_KEY,
            bucket_name=config.R2_BUCKET_NAME
        )

        # Clear any leftover hashes from a previous run
        log.info("正在清空旧的排队哈希集合...")
        await redis_client.delete(config.REDIS_QUEUED_SET)

    except Exception as e:
        log.error(f"无法初始化外部服务客户端: {e}")
        return

    # A bounded queue to act as a buffer between producer and consumers
    task_queue = asyncio.Queue(maxsize=args.queue_size)

    # The crawler acts as the producer
    crawler = SimpleCrawler(
        task_queue=task_queue,
        redis_client=redis_client,
        loop=loop
    )

    # The number of workers determines the download concurrency
    workers = [
        loop.create_task(metadata_downloader(task_queue, redis_client, es_handler, r2_handler))
        for _ in range(args.workers)
    ]

    # Start the periodic statistics printer
    stats_task = loop.create_task(print_stats(crawler, task_queue, redis_client))

    # Run the crawler on the specified port
    await crawler.run(port=args.port)
    log.info(f"爬虫正在监听端口 {crawler.transport.get_extra_info('sockname')[1]}")

    log.info(f"{args.workers} 个下载工作者已启动。按 Ctrl+C 停止。")

    # Handle graceful shutdown
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    log.info("正在关闭...")
    # Close external service connections
    await es_handler.close()
    await r2_handler.close()
    await redis_client.close()

    # Cancel all running tasks
    stats_task.cancel()
    for worker in workers:
        worker.cancel()
    crawler.stop()

    # Wait for all tasks to complete
    await asyncio.gather(*workers, return_exceptions=True)
    await task_queue.join()
    log.info("爬虫已优雅地关闭。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="一个简单而强大的 DHT 爬虫，用于下载种子元数据。")
    parser.add_argument("--port", type=int, default=config.DEFAULT_PORT, help="DHT 监听端口。")
    parser.add_argument("--workers", type=int, default=2000, help="并发元数据下载工作者的数量。")
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
