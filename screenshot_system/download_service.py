import libtorrent as lt
import time
import collections
import bencode2 as bencoder

class DownloaderService:
    def __init__(self, request_queue, result_dict, log_queue):
        self.log_queue = log_queue
        self.log("SERVICE", "Initializing downloader service...")

        # Use a plain dictionary for settings for compatibility with older libtorrent versions.
        settings = {
            'listen_interfaces': '0.0.0.0:0',
            'alert_mask': (
                lt.alert_category.status |
                lt.alert_category.storage |
                lt.alert_category.error |
                lt.alert_category.performance_warning
            ),
            'connections_limit': 200
        }

        self.ses = lt.session(settings)
        self.ses.add_dht_router("router.utorrent.com", 6881)
        self.ses.add_dht_router("router.bittorrent.com", 6881)
        self.ses.add_dht_router("dht.transmissionbt.com", 6881)

        self.request_queue = request_queue
        self.result_dict = result_dict
        self.handles = {} # infohash -> handle
        self.read_queue = collections.deque()
        self.log("SERVICE", "Downloader service initialized.")

    def log(self, infohash, message):
        """Puts a log message on the shared log queue."""
        self.log_queue.put(f"[{infohash}][Service] {message}")

    def add_torrent_with_metadata(self, infohash, metadata):
        """Adds a new torrent using its complete metadata."""
        if infohash in self.handles:
            return
        self.log(infohash, "Adding to session with metadata.")

        try:
            full_torrent_dict = {b'info': metadata}
            full_torrent_bencoded = bencoder.bencode(full_torrent_dict)
            ti = lt.torrent_info(full_torrent_bencoded)

            params = {
                'ti': ti,
                'save_path': '/tmp/',
                'storage_mode': lt.storage_mode_t(2), # storage_mode_sparse
            }
            handle = self.ses.add_torrent(params)
            self.handles[infohash] = handle
        except Exception as e:
            self.log(infohash, f"Error adding torrent with metadata: {e}")

    def run(self):
        """The main event loop for the service."""
        self.log("SERVICE", "Running main loop...")
        while True:
            while not self.request_queue.empty():
                req_type, payload = self.request_queue.get()

                if req_type == 'add_torrent':
                    infohash, metadata = payload
                    self.add_torrent_with_metadata(infohash, metadata)

                elif req_type == 'get_piece':
                    infohash, piece_index = payload
                    if infohash not in self.handles:
                        self.log(infohash, f"Warning: Piece {piece_index} requested before torrent was added.")
                        continue

                    handle = self.handles[infohash]
                    if handle.is_valid():
                        handle.piece_priority(piece_index, 7) # 7 is top priority in 1.x
                        self.log(infohash, f"Prioritized piece {piece_index}")

            alerts = self.ses.pop_alerts()
            for alert in alerts:
                if isinstance(alert, lt.piece_finished_alert):
                    h = alert.handle
                    if h.is_valid():
                        infohash = str(h.info_hash()).upper()
                        piece_index = alert.piece_index
                        self.log(infohash, f"Finished downloading piece {piece_index}. Queueing for read.")
                        self.read_queue.append((infohash, piece_index))

                elif isinstance(alert, lt.read_piece_alert):
                    h = alert.handle
                    if h.is_valid() and not alert.error:
                        infohash = str(h.info_hash()).upper()
                        piece_index = alert.piece
                        self.log(infohash, f"Successfully read piece {piece_index} from cache.")
                        key = ('piece', infohash, piece_index)
                        self.result_dict[key] = bytes(alert.buffer)
                    elif alert.error:
                        self.log(str(h.info_hash()).upper(), f"Failed to read piece {alert.piece}: {alert.error.message()}")

                elif isinstance(alert, lt.torrent_status_alert):
                    s = alert.status
                    h = alert.handle
                    if h.is_valid():
                        infohash = str(h.info_hash()).upper()
                        state_str = [
                            'queued', 'checking', 'downloading metadata',
                            'downloading', 'finished', 'seeding', 'allocating',
                            'checking fastresume'
                        ]
                        self.log(infohash,
                            f"State: {state_str[s.state]}, "
                            f"Peers: {s.num_peers}, Seeds: {s.num_seeds}, "
                            f"Progress: {s.progress * 100:.2f}%")

            if self.read_queue:
                infohash, piece_index = self.read_queue.popleft()
                if infohash in self.handles:
                    handle = self.handles[infohash]
                    if handle.is_valid():
                        handle.read_piece(piece_index)

            time.sleep(0.1)
