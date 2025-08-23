import libtorrent as lt
import time
import collections

def log(infohash, message):
    """Helper for consistent logging."""
    print(f"[{infohash}][Service] {message}")

class DownloaderService:
    """
    A long-running service that manages a single libtorrent session
    and downloads pieces on behalf of multiple worker processes.
    """
    def __init__(self, request_queue, result_dict):
        log("SERVICE", "Initializing downloader service...")
        settings = lt.high_performance_seed()
        settings['listen_interfaces'] = '0.0.0.0:0'
        # More alerts can be useful for debugging
        settings['alert_mask'] = (
            lt.alert_category.status |
            lt.alert_category.storage |
            lt.alert_category.error |
            lt.alert_category.performance_warning
        )
        settings['connections_limit'] = 200

        self.ses = lt.session(settings)
        self.ses.add_dht_router("router.utorrent.com", 6881)
        self.ses.add_dht_router("router.bittorrent.com", 6881)
        self.ses.add_dht_router("dht.transmissionbt.com", 6881)

        self.request_queue = request_queue
        self.result_dict = result_dict
        self.handles = {} # infohash -> handle
        # A queue of pieces we need to read from libtorrent's cache
        self.read_queue = collections.deque()
        log("SERVICE", "Downloader service initialized.")

    def add_torrent(self, infohash):
        """Adds a new torrent to the session for monitoring."""
        if infohash in self.handles:
            return
        log(infohash, "Adding to session.")
        params = {
            'save_path': '/tmp/',
            'storage_mode': lt.storage_mode_t(2), # storage_mode_sparse
        }
        handle = lt.add_magnet_uri(self.ses, f"magnet:?xt=urn:btih:{infohash}", params)
        self.handles[infohash] = handle

    def run(self):
        """The main event loop for the service."""
        log("SERVICE", "Running main loop...")
        while True:
            # 1. Check for and process incoming requests from workers
            while not self.request_queue.empty():
                req_type, payload = self.request_queue.get()

                if req_type == 'get_metadata':
                    infohash = payload
                    if infohash not in self.handles:
                        self.add_torrent(infohash)

                elif req_type == 'get_piece':
                    infohash, piece_index = payload
                    if infohash not in self.handles:
                        self.add_torrent(infohash)

                    handle = self.handles[infohash]
                    if handle.is_valid():
                        handle.piece_priority(piece_index, 7) # 7 is top priority in 1.x
                        log(infohash, f"Prioritized piece {piece_index}")

            # 2. Process alerts from libtorrent
            alerts = self.ses.pop_alerts()
            for alert in alerts:
                if isinstance(alert, lt.piece_finished_alert):
                    h = alert.handle
                    if h.is_valid():
                        infohash = str(h.info_hash()).upper()
                        piece_index = alert.piece_index
                        log(infohash, f"Finished downloading piece {piece_index}. Queueing for read.")
                        # Don't read the piece here, just queue the request.
                        # Reading is a blocking call.
                        self.read_queue.append((infohash, piece_index))

                elif isinstance(alert, lt.read_piece_alert):
                    h = alert.handle
                    if h.is_valid() and not alert.error:
                        infohash = str(h.info_hash()).upper()
                        piece_index = alert.piece
                        log(infohash, f"Successfully read piece {piece_index} from cache.")
                        # Store the data in the shared results dictionary
                        key = ('piece', infohash, piece_index)
                        self.result_dict[key] = bytes(alert.buffer)
                    elif alert.error:
                        log(str(h.info_hash()).upper(), f"Failed to read piece {alert.piece}: {alert.error.message()}")

                elif isinstance(alert, lt.metadata_received_alert):
                    h = alert.handle
                    if h.is_valid():
                        infohash = str(h.info_hash()).upper()
                        log(infohash, "Metadata received.")
                        ti = h.torrent_file()
                        if ti:
                            metadata = {
                                'piece_length': ti.piece_length(),
                                'num_pieces': ti.num_pieces(),
                            }
                            # Store metadata for the IO adapter to use
                            self.result_dict[('metadata', infohash)] = metadata

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
                        log(infohash,
                            f"State: {state_str[s.state]}, "
                            f"Peers: {s.num_peers}, Seeds: {s.num_seeds}, "
                            f"Progress: {s.progress * 100:.2f}%")

            # 3. Process the read queue
            if self.read_queue:
                infohash, piece_index = self.read_queue.popleft()
                if infohash in self.handles:
                    handle = self.handles[infohash]
                    if handle.is_valid():
                        # This is a non-blocking call that will result in a
                        # read_piece_alert later.
                        handle.read_piece(piece_index)

            time.sleep(0.1)
