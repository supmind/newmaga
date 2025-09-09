import asyncio
import binascii
import logging
import signal
import os
import argparse

from maga.crawler import Maga
from maga.downloader import get_metadata
from maga.utils import proper_infohash, BoundedSet, format_bytes
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

# 使用有界集合来跟踪已处理和已加入队列的 infohash，防止重复工作
# BoundedSet 是一个有最大大小限制的集合，当超过大小时，会自动丢弃最旧的元素
PROCESSED_INFOHASHES = BoundedSet(max_size=1_000_000)
QUEUED_INFOHASHES = BoundedSet(max_size=1_000_000)


async def metadata_downloader(task_queue, queued_hashes):
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
                PROCESSED_INFOHASHES.add(infohash_hex)

                # 从元数据中提取信息
                name = info.get(b'name', b'Unknown').decode(errors='ignore')

                # -- 中文注释 --
                # 将下载的元数据（info字典）编码为bencode格式，并保存为 .torrent 文件
                # 这是制作 .torrent 文件的标准方式
                torrent_data = bencode(info)
                file_path = os.path.join(DOWNLOAD_DIR, f"{infohash_hex}.torrent")
                with open(file_path, "wb") as f:
                    f.write(torrent_data)

                log.info(f"成功下载并保存元数据: Name: {name}, Infohash: {infohash_hex}")

        except asyncio.CancelledError:
            # 如果任务被取消，优雅地退出循环
            break
        except Exception:
            # 记录其他异常，但不要让工作者崩溃
            log.exception(f"处理 infohash 时出错: {infohash_hex}")
        finally:
            # 确保任务被标记为完成，并从排队集合中移除
            # 这样即使失败了，将来也有机会重试
            queued_hashes.remove(infohash_hex)
            task_queue.task_done()


class SimpleCrawler(Maga):
    """
    DHT 爬虫（生产者）。
    它负责发现 infohash 并将它们放入任务队列。
    """
    def __init__(self, task_queue, queued_hashes, processed_hashes, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_queue = task_queue
        self.queued_hashes = queued_hashes
        self.processed_hashes = processed_hashes

    async def handler(self, infohash, addr, peer_addr=None):
        """
        这个处理器在收到 `announce_peer` 消息时被调用。
        它将发现的任务放入队列，供工作者处理。
        """
        if not peer_addr:
            return

        infohash_hex = proper_infohash(infohash)

        # 只有当任务未被处理且未在队列中时，才加入队列
        if infohash_hex in self.queued_hashes or infohash_hex in self.processed_hashes:
            return

        # 先添加到排队集合，再放入任务队列
        self.queued_hashes.add(infohash_hex)
        try:
            self.task_queue.put_nowait((infohash, peer_addr))
        except asyncio.QueueFull:
            # 如果队列已满，从排队集合中移除，以便稍后有空间时可以重新排队
            self.queued_hashes.remove(infohash_hex)


async def print_stats(crawler, task_queue):
    """
    一个定期任务，用于打印爬虫和任务队列的统计信息。
    """
    while True:
        await asyncio.sleep(30)
        log.info(
            f"[统计] DHT 节点: {sum(len(b) for b in crawler.k_buckets)} | "
            f"队列大小: {task_queue.qsize()}/{task_queue.maxsize} | "
            f"已排队哈希: {len(QUEUED_INFOHASHES)} | "
            f"已处理哈希: {len(PROCESSED_INFOHASHES)}"
        )


async def main(args):
    """
    基于生产者-消费者模型的爬虫主入口点。
    """
    log.info("启动 DHT 爬虫 (生产者-消费者模型)...")
    loop = asyncio.get_running_loop()

    # 一个有界队列，用作生产者和消费者之间的缓冲区
    task_queue = asyncio.Queue(maxsize=args.queue_size)

    # 爬虫作为生产者
    crawler = SimpleCrawler(
        task_queue=task_queue,
        queued_hashes=QUEUED_INFOHASHES,
        processed_hashes=PROCESSED_INFOHASHES
    )

    # 下载工作者的数量决定了下载的并发度
    workers = [
        loop.create_task(metadata_downloader(task_queue, QUEUED_INFOHASHES))
        for _ in range(args.workers)
    ]

    # 在指定端口上运行爬虫
    await crawler.run(port=args.port)
    log.info(f"爬虫正在监听端口 {crawler.transport.get_extra_info('sockname')[1]}")

    # 启动定期的统计信息打印任务
    stats_task = loop.create_task(print_stats(crawler, task_queue))

    log.info(f"{args.workers} 个下载工作者已启动。按 Ctrl+C 停止。")

    # 处理优雅关闭
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    log.info("正在关闭...")
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
    parser.add_argument("--port", type=int, default=6881, help="DHT 监听端口。")
    parser.add_argument("--workers", type=int, default=200, help="并发元数据下载工作者的数量。")
    parser.add_argument("--queue-size", type=int, default=2000, help="任务队列的最大大小。")
    args = parser.parse_args()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        log.info("爬虫被用户停止。")
