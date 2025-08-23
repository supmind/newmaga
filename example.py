import asyncio
import logging
import os
import re
import time
import threading
import multiprocessing
import signal
from queue import Empty

from maga import Maga, get_metadata
from screenshot_system.download_service import DownloaderService
from screenshot_system.orchestrator import create_screenshots_from_stream
from screenshot_system.io_adapter import TorrentFileIO

# --- Start of Classification System (Unchanged) ---
CLASSIFICATION_RULES = {
    'japanese_av': {'type': 'hybrid', 'regex': r'\b[A-Z]+-\d+\b', 'keywords': ['jav', 'fc2']},
    'chinese_homemade': {'type': 'keywords', 'words': ['自拍', '探花', '寻花', '原创', '泄密', '流出', '调教', '露出', '口交', '啪啪啪', '做爱', '操', '插', '射', '淫', '骚', '逼', '穴', '屌', '后庭', '潮喷', '自慰', '群P', '3P', '乱伦', '奸', '学生', '少妇', '人妻', '女神', '嫩妹', '小姐姐', '美女', '学妹', '网红', '名媛', '外围', '舞姬', '老师', '夫妻', '情侣', '国产', '國產', '91', '精东', '麻豆', '天美', '海角', '推特']}
}
def classify_torrent(name):
    name_lower = name.lower()
    for category, rule in CLASSIFICATION_RULES.items():
        rule_type = rule.get('type')
        if rule_type == 'hybrid':
            if re.search(rule['regex'], name): return category
            for word in rule['keywords']:
                if word in name_lower: return category
        elif rule_type == 'keywords':
            for word in rule['words']:
                if word.lower() in name_lower: return category
    return None
# --- End of Classification System ---

def run_screenshot_task(infohash: str, metadata: dict, target_file_index: int, file_size: int, stats_queue, request_queue, result_dict):
    """
    This process is self-contained. It creates its own downloader
    and handles one torrent from start to finish.
    """
    worker_id = os.getpid()
    print(f"[Worker:{worker_id}] Started for {infohash}")
    print(f"[Worker:{worker_id}]   - File Index: {target_file_index}")
    print(f"[Worker:{worker_id}]   - File Size: {file_size}")
    try:
        # Tell the service to start managing this torrent
        print(f"[Worker:{worker_id}] Sending 'add_torrent' request to service...")
        request_queue.put(('add_torrent', (infohash, metadata)))
        print(f"[Worker:{worker_id}] Request sent. Initializing IO adapter...")

        # The IO adapter uses the downloader service via the queues
        io_adapter = TorrentFileIO(infohash, metadata, target_file_index, file_size, request_queue, result_dict)
        create_screenshots_from_stream(io_adapter, infohash, stats_queue)
    except Exception as e:
        print(f"[Worker:{worker_id}] Unhandled error for {infohash}: {e}")
    finally:
        # No need to close a downloader, the service handles it.
        print(f"[Worker:{worker_id}] Finished for {infohash}")


def statistics_worker(queue):
    total_screenshots = 0
    while True:
        time.sleep(60)
        period_screenshots = 0
        while not queue.empty():
            try:
                queue.get_nowait()
                period_screenshots += 1
            except Empty: break
        total_screenshots += period_screenshots
        print(f"[STATS] Last 60s: {period_screenshots} screenshots. Total: {total_screenshots} screenshots.")

class Crawler(Maga):
    def __init__(self, stats_queue, request_queue, result_dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_infohashes = set()
        self.stats_queue = stats_queue
        self.request_queue = request_queue
        self.result_dict = result_dict
        self.worker_processes = []

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        if infohash in self.processed_infohashes: return
        self.processed_infohashes.add(infohash)

        loop = asyncio.get_event_loop()
        try:
            metadata = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)
        except Exception:
            return
        if not metadata: return

        torrent_name_bytes = metadata.get(b'name')
        if not torrent_name_bytes: return
        torrent_name_str = torrent_name_bytes.decode('utf-8', 'ignore')
        if not classify_torrent(torrent_name_str): return

        print(f"[Crawler] Found classified torrent: '{torrent_name_str}'")

        target_file_index, largest_size = -1, 0
        files_metadata = metadata.get(b'files')
        if files_metadata:
            for i, f in enumerate(files_metadata):
                path_parts_bytes = f.get(b'path', [])
                if not path_parts_bytes: continue
                filename = path_parts_bytes[-1].decode('utf-8', 'ignore')
                if filename.lower().endswith('.mp4'):
                    file_size = f.get(b'length', 0)
                    if file_size > largest_size:
                        largest_size, target_file_index = file_size, i
        else:
            if torrent_name_str.lower().endswith('.mp4'):
                largest_size, target_file_index = metadata.get(b'length', 0), 0

        if target_file_index != -1:
            print(f"[Crawler] Handing off task for {infohash}")
            args = (
                infohash,
                metadata,
                target_file_index,
                largest_size,
                self.stats_queue,
                self.request_queue,
                self.result_dict
            )
            p = multiprocessing.Process(target=run_screenshot_task, args=args)
            p.daemon = True
            p.start()
            self.worker_processes.append(p)

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)

    # Use a multiprocessing Manager to create shared objects
    manager = multiprocessing.Manager()
    stats_queue = manager.Queue()
    download_request_queue = manager.Queue()
    download_result_dict = manager.dict()

    # Start statistics thread
    stats_thread = threading.Thread(target=statistics_worker, args=(stats_queue,), daemon=True)
    stats_thread.start()

    # Create and start the downloader service
    print("[Main] Starting Downloader Service...")
    downloader_service = DownloaderService(download_request_queue, download_result_dict)
    downloader_process = multiprocessing.Process(target=downloader_service.run, daemon=True)
    downloader_process.start()

    # Create the crawler instance
    crawler = Crawler(stats_queue, download_request_queue, download_result_dict)

    # Define a graceful shutdown handler
    def shutdown_handler(signum, frame):
        print("\n[Main] Shutdown signal received. Cleaning up...")

        # 1. Terminate downloader service
        if downloader_process.is_alive():
            print("[Main] Terminating downloader service...")
            downloader_process.terminate()
            downloader_process.join(timeout=5)

        # 2. Terminate worker processes
        # Make a copy as the list could be modified elsewhere
        for worker in list(crawler.worker_processes):
            if worker.is_alive():
                print(f"[Main] Terminating worker {worker.pid}...")
                worker.terminate()
                worker.join(timeout=5)

        # 3. Stop the crawler's event loop
        print("[Main] Stopping crawler...")
        if crawler.loop and crawler.loop.is_running():
            crawler.stop()

        print("[Main] Cleanup complete. Exiting.")

    # Register the signal handlers
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Start the crawler
    print("[Main] Starting Crawler...")
    crawler.run(6881)

    print("[Main] Crawler has stopped. Exiting.")
