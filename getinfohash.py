import asyncio
import logging
import signal
from maga.crawler import Maga

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DiagnosticCrawler(Maga):
    """
    A crawler that doesn't handle infohashes, used for diagnostics.
    """
    async def handle_get_peers(self, infohash, addr):
        pass

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        pass

async def print_diagnostics(crawler: Maga):
    """
    A background task to periodically print diagnostic statistics.
    """
    while True:
        try:
            await asyncio.sleep(30)  # Print stats every 30 seconds

            # --- Routing Table Stats ---
            total_nodes = 0
            non_empty_buckets = 0
            for i, bucket in enumerate(crawler.k_buckets):
                size = len(bucket)
                if size > 0:
                    total_nodes += size
                    non_empty_buckets += 1

            # --- Node Queue Stats ---
            queue_size = crawler.node_queue.qsize()
            max_queue_size = crawler.node_queue.maxsize

            logging.info("="*20 + " Crawler Diagnostics " + "="*20)
            logging.info(f"[Routing Table] Total Nodes: {total_nodes} | Non-empty Buckets: {non_empty_buckets}/{len(crawler.k_buckets)}")
            logging.info(f"[Node Queue]    Current Size: {queue_size} / {max_queue_size}")
            logging.info("="*59 + "\n")

        except asyncio.CancelledError:
            logging.info("Diagnostics task cancelled.")
            break
        except Exception as e:
            logging.error(f"Error in diagnostics task: {e}", exc_info=True)

async def main():
    loop = asyncio.get_running_loop()

    # Create an instance of our diagnostic crawler
    crawler = DiagnosticCrawler(node_processor_concurrency=20)

    # Start the crawler
    await crawler.run(port=0)
    logging.info("Crawler is running. Press Ctrl+C to stop.")

    # Start the diagnostic task
    diag_task = asyncio.create_task(print_diagnostics(crawler))

    # Set up signal handler for graceful shutdown
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)

    await stop

    logging.info("Stopping crawler...")
    diag_task.cancel()
    crawler.stop()
    await asyncio.gather(diag_task, return_exceptions=True)
    logging.info("Crawler and diagnostics task stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
