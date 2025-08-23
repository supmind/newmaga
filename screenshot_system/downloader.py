import libtorrent as lt
import time
from queue import Empty

class DownloaderService:
    def __init__(self):
        settings = lt.high_performance_seed()
        settings['listen_interfaces'] = '0.0.0.0:6881'
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
        self.active_reads = {} # piece_hash -> list of request_ids

    def get_or_add_handle(self, infohash):
        if infohash in self.handles:
            return self.handles.get(infohash)

        magnet_link = f"magnet:?xt=urn:btih:{infohash}"
        params = {'save_path': '/tmp/', 'storage_mode': lt.storage_mode_t(2)}
        handle = self.ses.add_magnet_uri(magnet_link, params)
        self.handles[infohash] = handle
        return handle

    def process_alerts(self, result_queue):
        alerts = self.ses.pop_alerts()
        for alert in alerts:
            if isinstance(alert, lt.read_piece_alert):
                request_id = alert.user_data
                result_queue.put({'request_id': request_id, 'data': bytes(alert.buffer), 'piece': alert.piece})
            elif isinstance(alert, lt.torrent_error_alert):
                print(f"[DownloaderService] Torrent Error: {alert.error.message()}")

    def run(self, task_queue, result_queue):
        print("[DownloaderService] Started.")
        while True:
            try:
                task = task_queue.get(timeout=0.2)
                task_type = task.get('type')

                if task_type == 'DOWNLOAD_RANGE':
                    request_id = task['request_id']
                    infohash = task['infohash']
                    handle = self.get_or_add_handle(infohash)

                    if not handle or not handle.is_valid():
                        result_queue.put({'request_id': request_id, 'error': 'Invalid handle'})
                        continue

                    if not handle.has_metadata():
                        task_queue.put(task) # Re-queue
                        continue

                    ti = handle.torrent_file()
                    piece_size = ti.piece_length()
                    file_offset = ti.files().file_offset(task['file_index'])
                    abs_offset = file_offset + task['offset']

                    start_piece, _ = divmod(abs_offset, piece_size)
                    end_piece, _ = divmod(abs_offset + task['size'] - 1, piece_size)

                    for p_idx in range(start_piece, end_piece + 1):
                        # The user_data field is used to associate the alert with our request_id
                        handle.read_piece(p_idx, request_id)

            except Empty:
                pass

            self.ses.post_torrent_updates()
            self.process_alerts(result_queue)
