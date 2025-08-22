import libtorrent as lt
import time
from queue import Queue, Empty

class DownloaderService:
    def __init__(self, task_queue, result_queue):
        self.task_queue = task_queue
        self.result_queue = result_queue

        settings = lt.high_performance_seed()
        settings['listen_interfaces'] = '0.0.0.0:0'
        settings['alert_mask'] = (
            lt.alert_category.status |
            lt.alert_category.storage |
            lt.alert_category.error
        )
        self.ses = lt.session(settings)
        self.ses.add_dht_router("router.utorrent.com", 6881)
        self.ses.add_dht_router("router.bittorrent.com", 6881)
        self.ses.add_dht_router("dht.transmissionbt.com", 6881)

        self.handles = {}

    def get_or_add_handle(self, infohash):
        if infohash in self.handles:
            handle = self.handles.get(infohash)
            if handle and handle.is_valid():
                return handle

        print(f"[Downloader] Adding new torrent: {infohash}")
        magnet_link = f"magnet:?xt=urn:btih:{infohash}"
        params = {'save_path': '/tmp/', 'storage_mode': lt.storage_mode_t(2)}
        handle = lt.add_magnet_uri(self.ses, magnet_link, params)
        self.handles[infohash] = handle

        meta_start_time = time.time()
        while not handle.has_metadata():
            self.ses.wait_for_alert(500)
            if time.time() - meta_start_time > 60:
                print(f"[Downloader] Timeout getting metadata for {infohash}")
                return None
        print(f"[Downloader] Metadata received for {infohash}")
        return handle

    def run(self):
        print("[DownloaderService] Thread Started.")
        while True:
            try:
                task = self.task_queue.get(timeout=0.2)
                request_id = task.get('request_id')
                infohash = task.get('infohash')
                print(f"[Downloader] Received task {request_id} for {infohash}")

                handle = self.get_or_add_handle(infohash)
                if not handle:
                    self.result_queue.put({'request_id': request_id, 'error': 'Could not get handle'})
                    continue

                ti = handle.torrent_file()
                piece_size = ti.piece_length()
                file_offset = ti.files().file_offset(task['file_index'])
                abs_offset = file_offset + task['offset']

                start_piece, _ = divmod(abs_offset, piece_size)
                end_piece, _ = divmod(abs_offset + task['size'] - 1, piece_size)

                pieces_needed = set(range(start_piece, end_piece + 1))

                for p_idx in pieces_needed:
                    handle.piece_priority(p_idx, 7)

                download_start_time = time.time()
                pieces_done = set()
                while pieces_done != pieces_needed:
                    if time.time() - download_start_time > 180:
                        break

                    alerts = self.ses.pop_alerts()
                    for alert in alerts:
                        if isinstance(alert, lt.piece_finished_alert):
                            if alert.piece_index in pieces_needed:
                                pieces_done.add(alert.piece_index)

                if pieces_done != pieces_needed:
                    self.result_queue.put({'request_id': request_id, 'error': 'Download timeout'})
                    continue

                all_data = {}
                for piece_index in sorted(list(pieces_needed)):
                    handle.read_piece(piece_index)
                    alert = self.ses.wait_for_alert(10000)
                    if isinstance(alert, lt.read_piece_alert) and alert.piece == piece_index:
                         all_data[piece_index] = bytes(alert.buffer)
                    else:
                        all_data = None
                        break

                if all_data is None:
                    self.result_queue.put({'request_id': request_id, 'error': 'Failed to read pieces'})
                    continue

                full_chunk = b"".join(all_data[i] for i in sorted(all_data))
                p_offset = start_piece * piece_size
                start = abs_offset - p_offset
                end = start + task['size']
                data = full_chunk[start:end]

                self.result_queue.put({'request_id': request_id, 'data': data})

            except Empty:
                self.ses.post_torrent_updates()
            except Exception as e:
                print(f"[DownloaderService] Unhandled error: {e}")
