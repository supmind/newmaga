import libtorrent as lt
import time
from queue import Empty

def downloader_service_main(task_queue, result_queue):
    """The main entry point for the downloader process."""
    service = DownloaderService()
    service.run(task_queue, result_queue)

class DownloaderService:
    def __init__(self):
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
        self.active_requests = {}

    def get_or_add_handle(self, infohash):
        if infohash in self.handles:
            handle = self.handles.get(infohash)
            if handle and handle.is_valid():
                return handle

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
        return handle

    def process_alerts(self, result_queue):
        alerts = self.ses.pop_alerts()
        for alert in alerts:
            if isinstance(alert, lt.read_piece_alert):
                request_id = alert.user_data
                if request_id in self.active_requests:
                    request = self.active_requests[request_id]
                    request['pieces_data'][alert.piece] = bytes(alert.buffer)

                    if len(request['pieces_data']) == len(request['pieces_needed']):
                        full_chunk = b"".join(request['pieces_data'][i] for i in sorted(request['pieces_data']))

                        p_size = request['piece_size']
                        p_offset = request['start_piece'] * p_size
                        start = request['abs_offset'] - p_offset
                        end = start + request['size']
                        data = full_chunk[start:end]

                        result_queue.put({'request_id': request_id, 'data': data})
                        del self.active_requests[request_id]

            elif isinstance(alert, lt.torrent_error_alert):
                print(f"[DownloaderService] Torrent Error: {alert.error.message()}")

    def run(self, task_queue, result_queue):
        print("[DownloaderService] Started.")
        while True:
            try:
                task = task_queue.get(timeout=0.2)
                request_id = task.get('request_id')
                infohash = task.get('infohash')

                handle = self.get_or_add_handle(infohash)
                if not handle:
                    result_queue.put({'request_id': request_id, 'error': 'Could not get handle'})
                    continue

                ti = handle.torrent_file()
                piece_size = ti.piece_length()
                file_offset = ti.files().file_offset(task['file_index'])
                abs_offset = file_offset + task['offset']

                start_piece, _ = divmod(abs_offset, piece_size)
                end_piece, _ = divmod(abs_offset + task['size'] - 1, piece_size)

                pieces_needed = set(range(start_piece, end_piece + 1))

                self.active_requests[request_id] = {
                    'pieces_needed': pieces_needed, 'pieces_data': {},
                    'abs_offset': abs_offset, 'size': task['size'],
                    'piece_size': piece_size, 'start_piece': start_piece
                }

                for p_idx in pieces_needed:
                    handle.read_piece(p_idx, request_id)

            except Empty:
                pass
            except Exception as e:
                print(f"[DownloaderService] Unhandled error: {e}")

            self.ses.post_torrent_updates()
            self.process_alerts(result_queue)
