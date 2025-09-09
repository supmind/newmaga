import asyncio
import binascii
import logging
import signal
import os
import argparse
import configparser
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from maga.crawler import Maga
from maga.downloader import get_metadata
from maga.utils import proper_infohash, BoundedSet, format_bytes
from fastbencode import bencode


# -- 新增模块 --
# 全局异步队列，用于解耦元数据下载和ES索引
ES_QUEUE = asyncio.Queue()

def load_config(path='config.ini'):
    """加载配置文件"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"配置文件 '{path}' 未找到。请从 'config.ini.template' 创建。")
    config = configparser.ConfigParser()
    config.read(path)
    return config

async def upload_to_r2(config, infohash_hex, torrent_data):
    """将种子文件上传到Cloudflare R2"""
    try:
        r2_config = config['R2']
        s3_client = boto3.client(
            service_name='s3',
            endpoint_url=r2_config['endpoint_url'],
            aws_access_key_id=r2_config['access_key_id'],
            aws_secret_access_key=r2_config['secret_access_key'],
            region_name='auto'  # R2 specific
        )
        file_name = f"{infohash_hex}.torrent"
        s3_client.put_object(
            Bucket=r2_config['bucket_name'],
            Key=file_name,
            Body=torrent_data
        )
        log.info(f"成功上传 {file_name} 到 R2 存储桶 {r2_config['bucket_name']}")
    except ClientError as e:
        log.error(f"上传到 R2 失败: {e}")
    except Exception:
        log.exception("上传到 R2 时发生未知错误")

async def elasticsearch_bulk_worker(config, queue):
    """从队列中获取数据并批量索引到Elasticsearch"""
    es_config = config['Elasticsearch']
    batch_size = int(es_config.get('batch_size', 100))

    # 根据配置初始化ES客户端
    es_client_args = {'hosts': es_config['hosts']}
    if 'api_key' in es_config and es_config['api_key']:
        es_client_args['api_key'] = es_config['api_key']
    if 'ca_certs' in es_config and es_config['ca_certs']:
        es_client_args['ca_certs'] = es_config['ca_certs']

    es_client = AsyncElasticsearch(**es_client_args)

    log.info(f"Elasticsearch批量工作者已启动，批处理大小为 {batch_size}")

    while True:
        try:
            batch = []
            # 等待第一个项目
            item = await queue.get()
            batch.append(item)
            queue.task_done()

            # 收集更多项目，直到达到批处理大小或超时
            while len(batch) < batch_size:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=5.0)
                    batch.append(item)
                    queue.task_done()
                except asyncio.TimeoutError:
                    break  # 如果超时，则处理当前批次

            if batch:
                actions = [
                    {
                        "_index": es_config['index_name'],
                        "_id": doc["infohash"],
                        "_source": doc
                    }
                    for doc in batch
                ]
                success, failed = await async_bulk(es_client, actions, raise_on_error=False)
                log.info(f"成功索引 {success} 个文档到 Elasticsearch。失败: {failed} 个。")
                if failed:
                    log.error(f"索引失败详情: {failed}")

        except asyncio.CancelledError:
            log.info("Elasticsearch工作者正在关闭...")
            break
        except Exception:
            log.exception("Elasticsearch工作者遇到错误")
            # 在继续之前等待一小段时间，以避免快速失败循环
            await asyncio.sleep(5)

    await es_client.close()
    log.info("Elasticsearch客户端已关闭。")


# -- 配置日志记录 --
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


async def metadata_downloader(config, task_queue, es_queue, queued_hashes):
    """
    元数据下载器（消费者/工作者）。
    它从任务队列中获取任务，下载元数据，上传到R2，并放入ES队列。
    """
    while True:
        infohash_hex = "N/A"  # Default value
        try:
            infohash, peer_addr = await task_queue.get()
            infohash_hex = proper_infohash(infohash)

            loop = asyncio.get_running_loop()
            info = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop, timeout=10)

            if info:
                PROCESSED_INFOHASHES.add(infohash_hex)
                torrent_data = bencode(info)

                # 异步运行R2上传
                # 注意：boto3是同步的，在asyncio事件循环中直接调用会阻塞。
                # 理想情况下应使用像 aioboto3 这样的异步库，或使用 run_in_executor。
                # 为简单起见，我们暂时直接调用，但在高负载下这可能成为瓶颈。
                await upload_to_r2(config, infohash_hex, torrent_data)

                # 准备 ES 文档
                name = info.get(b'name', b'Unknown').decode(errors='ignore')
                files = []
                total_size = 0
                if b'files' in info:  # 多文件种子
                    for f in info[b'files']:
                        try:
                            path = b'/'.join(f.get(b'path.utf-8', f.get(b'path',[]))).decode('utf-8', errors='ignore')
                        except TypeError:
                            path = info.get(b'name', b'Unknown').decode(errors='ignore')
                        size = f.get(b'length', 0)
                        files.append({'path': path, 'size': size})
                        total_size += size
                else:  # 单文件种子
                    size = info.get(b'length', 0)
                    files.append({'path': name, 'size': size})
                    total_size = size

                es_doc = {
                    "infohash": infohash_hex,
                    "name": name,
                    "files": files,
                    "total_size": total_size,
                    "discovered_at": datetime.now(timezone.utc).isoformat()
                }

                # 放入 ES 队列
                await es_queue.put(es_doc)

                log.info(f"成功处理 {infohash_hex} (Name: {name})，已上传至R2并排队等待索引。")

        except asyncio.CancelledError:
            break
        except Exception:
            log.exception(f"处理 infohash 时出错: {infohash_hex}")
        finally:
            # 确保任务被标记为完成，并从排队集合中移除
            if infohash_hex != "N/A":
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


async def print_stats(crawler, task_queue, es_queue):
    """
    一个定期任务，用于打印爬虫和任务队列的统计信息。
    """
    while True:
        await asyncio.sleep(30)
        log.info(
            f"[统计] DHT 节点: {sum(len(b) for b in crawler.k_buckets)} | "
            f"下载队列: {task_queue.qsize()}/{task_queue.maxsize} | "
            f"ES 队列: {es_queue.qsize()} | "
            f"已排队: {len(QUEUED_INFOHASHES)} | "
            f"已处理: {len(PROCESSED_INFOHASHES)}"
        )


async def main(args):
    """
    基于生产者-消费者模型的爬虫主入口点。
    """
    log.info("正在加载配置文件...")
    try:
        config = load_config()
    except FileNotFoundError as e:
        log.error(e)
        return

    log.info("启动 DHT 爬虫 (生产者-消费者模型)...")
    loop = asyncio.get_running_loop()

    # 主任务队列，爬虫 -> 下载器
    task_queue = asyncio.Queue(maxsize=args.queue_size)
    # ES_QUEUE 在全局定义

    # 爬虫作为生产者
    crawler = SimpleCrawler(
        task_queue=task_queue,
        queued_hashes=QUEUED_INFOHASHES,
        processed_hashes=PROCESSED_INFOHASHES
    )

    # 下载工作者的数量决定了下载的并发度
    downloader_workers = [
        loop.create_task(metadata_downloader(config, task_queue, ES_QUEUE, QUEUED_INFOHASHES))
        for _ in range(args.workers)
    ]

    # 启动 Elasticsearch 批量工作者
    es_worker = loop.create_task(elasticsearch_bulk_worker(config, ES_QUEUE))

    # 在指定端口上运行爬虫
    await crawler.run(port=args.port)
    log.info(f"爬虫正在监听端口 {crawler.transport.get_extra_info('sockname')[1]}")

    # 更新统计任务以包含ES队列大小
    stats_task = loop.create_task(print_stats(crawler, task_queue, ES_QUEUE))

    log.info(f"{args.workers} 个下载工作者和1个ES工作者已启动。按 Ctrl+C 停止。")

    # 处理优雅关闭
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    log.info("正在关闭...")
    # 取消所有后台任务
    tasks = [stats_task, es_worker] + downloader_workers
    for task in tasks:
        task.cancel()
    crawler.stop()

    # 等待所有任务完成
    await asyncio.gather(*tasks, return_exceptions=True)
    # 等待队列处理完毕
    await task_queue.join()
    # 在关闭ES客户端之前，确保队列中的剩余项目也被处理
    await es_worker
    log.info("爬虫已优雅地关闭。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DHT 爬虫，用于下载种子元数据、上传到R2并索引到Elasticsearch。")
    parser.add_argument("--port", type=int, default=6881, help="DHT 监听端口。")
    parser.add_argument("--workers", type=int, default=50, help="并发元数据下载工作者的数量。")
    parser.add_argument("--queue-size", type=int, default=1000, help="任务队列的最大大小。")
    args = parser.parse_args()

    # 为uvloop设置策略，如果可用
    try:
        import uvloop
        uvloop.install()
        log.info("使用 uvloop 事件循环。")
    except ImportError:
        log.info("未找到 uvloop，使用默认的 asyncio 事件循环。")

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        log.info("爬虫被用户停止。")
