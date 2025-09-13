import asyncio
import binascii
import logging
import signal
import os
import argparse
import aiohttp
import io

# Third-party imports
from elasticsearch_dsl import async_connections
import aioboto3
from fastbencode import bencode

# Local imports
import config
from maga.crawler import Maga
from maga.downloader import get_metadata
from maga.es import TorrentMetadata, FileInfo
from maga.utils import proper_infohash, BoundedSet, format_bytes

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# Bounded sets to track processed and queued infohashes
PROCESSED_INFOHASHES = BoundedSet(max_size=1_000_000)
QUEUED_INFOHASHES = BoundedSet(max_size=1_000_000)


async def submit_screenshot_task(infohash: str, torrent_data: bytes):
    """
    Submits a task to the screenshot service using in-memory torrent data.
    """
    headers = {
        'accept': 'application/json',
        'X-API-Key': config.SCREENSHOT_API_KEY
    }

    form_data = aiohttp.FormData()
    form_data.add_field('infohash', infohash)
    form_data.add_field(
        'torrent_file',
        io.BytesIO(torrent_data),
        filename=f"{infohash}.torrent",
        content_type='application/x-bittorrent'
    )

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(config.SCREENSHOT_URL, headers=headers, data=form_data) as response:
                if response.status == 200:
                    log.info(f"Successfully submitted screenshot task for {infohash}.")
                else:
                    log.error(f"Failed to submit screenshot task for {infohash}. Status: {response.status}, Response: {await response.text()}")
    except aiohttp.ClientError as e:
        log.error(f"An error occurred while submitting screenshot task for {infohash}: {e}")


async def metadata_downloader(task_queue, queued_hashes, r2_client, es_client):
    """
    Worker task to download metadata, upload to R2, and index in Elasticsearch.
    """
    while True:
        infohash_hex = None
        try:
            infohash, peer_addr = await task_queue.get()
            infohash_hex = proper_infohash(infohash)

            loop = asyncio.get_running_loop()
            info = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop, timeout=10)

            if not info:
                continue

            PROCESSED_INFOHASHES.add(infohash_hex)

            # --- Prepare Data ---
            name = info.get(b'name', b'Unknown').decode(errors='ignore')
            torrent_data = bencode({b'info': info})

            # --- R2 Upload ---
            if all([r2_client, config.R2_BUCKET_NAME]):
                try:
                    await r2_client.put_object(
                        Bucket=config.R2_BUCKET_NAME,
                        Key=f"{infohash_hex}.torrent",
                        Body=torrent_data
                    )
                    log.info(f"Uploaded {infohash_hex}.torrent to R2 bucket: {config.R2_BUCKET_NAME}")
                except Exception:
                    log.exception(f"Failed to upload {infohash_hex}.torrent to R2.")

            # --- Elasticsearch Indexing ---
            try:
                files = []
                total_size = 0
                if b'files' in info:
                    for f in info[b'files']:
                        path_parts = [part.decode(errors='ignore') for part in f.get(b'path', [])]
                        path = "/".join(path_parts)
                        length = f.get(b'length', 0)
                        files.append(FileInfo(path=path, length=length))
                        total_size += length
                else:
                    total_size = info.get(b'length', 0)

                doc = TorrentMetadata(
                    meta={'id': infohash_hex, 'index': config.ES_INDEX},
                    info_hash=infohash_hex,
                    name=name,
                    files=files,
                    total_size=total_size
                )
                await doc.save(using=es_client)
                log.info(f"Indexed metadata for: {name} ({infohash_hex})")
            except Exception:
                log.exception(f"Failed to index metadata for {infohash_hex}.")

            # --- Screenshot Submission ---
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
                loop.create_task(submit_screenshot_task(infohash_hex, torrent_data))

        except asyncio.CancelledError:
            break
        except Exception:
            if infohash_hex:
                log.exception(f"Error processing infohash: {infohash_hex}")
            else:
                log.exception("Error in metadata_downloader before infohash was retrieved.")
        finally:
            if infohash_hex:
                queued_hashes.remove(infohash_hex)
            task_queue.task_done()


class SimpleCrawler(Maga):
    """
    The producer part of the crawler. Discovers infohashes and puts them
    into the task queue for the workers.
    """
    def __init__(self, task_queue, queued_hashes, processed_hashes, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_queue = task_queue
        self.queued_hashes = queued_hashes
        self.processed_hashes = processed_hashes

    async def handler(self, infohash, addr, peer_addr=None):
        if not peer_addr:
            return

        infohash_hex = proper_infohash(infohash)

        if infohash_hex in self.queued_hashes or infohash_hex in self.processed_hashes:
            return

        self.queued_hashes.add(infohash_hex)
        try:
            self.task_queue.put_nowait((infohash, peer_addr))
        except asyncio.QueueFull:
            self.queued_hashes.remove(infohash_hex)


async def main(args):
    """
    Main entry point. Sets up clients, crawler, and workers.
    """
    log.info("Starting DHT crawler...")
    loop = asyncio.get_running_loop()

    # --- Initialize Clients ---
    log.info(f"Connecting to Elasticsearch at {config.ES_HOSTS}")
    auth_params = {}
    if config.ES_USERNAME and config.ES_PASSWORD:
        auth_params['http_auth'] = (config.ES_USERNAME, config.ES_PASSWORD)
        log.info("Using Elasticsearch authentication.")

    es_client = async_connections.create_connection(
        hosts=[config.ES_HOSTS],
        **auth_params
    )

    # Ensure the Elasticsearch index is created with the correct mappings
    await TorrentMetadata.init(using=es_client)
    log.info(f"Elasticsearch index '{config.ES_INDEX}' is ready.")

    # Setup R2 client
    session = aioboto3.Session()
    r2_client = None # Will be created inside the async with block

    async with session.client(
        "s3",
        endpoint_url=config.R2_ENDPOINT_URL,
        aws_access_key_id=config.R2_ACCESS_KEY_ID,
        aws_secret_access_key=config.R2_SECRET_ACCESS_KEY
    ) as r2_client:

        task_queue = asyncio.Queue(maxsize=args.queue_size)

        crawler = SimpleCrawler(
            task_queue=task_queue,
            queued_hashes=QUEUED_INFOHASHES,
            processed_hashes=PROCESSED_INFOHASHES
        )

        workers = [
            loop.create_task(metadata_downloader(
                task_queue,
                QUEUED_INFOHASHES,
                r2_client,
                es_client
            ))
            for _ in range(args.workers)
        ]

        await crawler.run(port=args.port)
        log.info(f"Crawler is running on port {crawler.transport.get_extra_info('sockname')[1]}")

        log.info(f"{args.workers} download workers started. Press Ctrl+C to stop.")

        stop = asyncio.Future()
        loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
        await stop

        log.info("Shutting down...")
        for worker in workers:
            worker.cancel()
        crawler.stop()
        await asyncio.gather(*workers, return_exceptions=True)
        await task_queue.join()

    # --- Close connections ---
    await async_connections.get_connection().close()
    log.info("Crawler shut down gracefully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A DHT crawler for downloading and indexing torrent metadata.")
    parser.add_argument("--port", type=int, default=config.CRAWLER_PORT, help="DHT listening port.")
    parser.add_argument("--workers", type=int, default=config.CRAWLER_WORKERS, help="Number of concurrent metadata download workers.")
    parser.add_argument("--queue-size", type=int, default=config.CRAWLER_QUEUE_SIZE, help="Maximum size of the task queue.")
    args = parser.parse_args()

    # Validate R2 configuration
    if not all([config.R2_ACCOUNT_ID, config.R2_ACCESS_KEY_ID, config.R2_SECRET_ACCESS_KEY, config.R2_BUCKET_NAME]):
        log.warning("R2 configuration is incomplete. File uploading will be disabled.")
        log.warning("Please set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, and R2_BUCKET_NAME.")

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        log.info("Crawler stopped by user.")
