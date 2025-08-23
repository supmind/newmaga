import libtorrent as lt
import time
import collections
import bencode2 as bencoder
import datetime
import traceback

LOG_FILE = 'downloader_service.log'

def log_to_file(message):
    """Appends a message to the service log file."""
    with open(LOG_FILE, 'a') as f:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        f.write(f"{timestamp} - {message}\n")

class DownloaderService:
    def __init__(self, request_queue, result_dict):
        with open(LOG_FILE, 'w'):
            pass

        self.log("SERVICE", "Initializing downloader service for libtorrent 2.x...")

        # Use settings_pack for libtorrent 2.x
        settings = lt.settings_pack()
        settings.set_int(lt.setting_name.alert_mask, lt.alert_category.all)
        settings.set_str(lt.setting_name.listen_interfaces, '0.0.0.0:0')
        settings.set_int(lt.setting_name.connections_limit, 200)

        self.ses = lt.session(params=settings)
        self.ses.add_dht_router("router.utorrent.com", 6881)
        self.ses.add_dht_router("router.bittorrent.com", 6881)
        self.ses.add_dht_router("dht.transmissionbt.com", 6881)

        self.request_queue = request_queue
        self.result_dict = result_dict
        self.handles = {} # infohash -> handle
        self.read_queue = collections.deque()
        self.log("SERVICE", "Downloader service initialized.")

    def log(self, infohash, message):
        """Logs a message to the dedicated service log file."""
        log_to_file(f"[{infohash}][Service] {message}")

    def add_torrent_with_metadata(self, infohash, metadata):
        """Adds a new torrent using its complete metadata."""
        if infohash in self.handles:
            return
        self.log(infohash, "Adding to session with metadata.")

        try:
            full_torrent_dict = {b'info': metadata}
            full_torrent_bencoded = bencoder.bencode(full_torrent_dict)
            ti = lt.torrent_info(full_torrent_bencoded)

            params = lt.add_torrent_params()
            params.ti = ti
            params.save_path = '/tmp/'
            params.storage_mode = lt.storage_mode_t.storage_mode_sparse

            handle = self.ses.add_torrent(params)
            self.handles[infohash] = handle
            self.log(infohash, "Successfully added torrent to session.")
        except Exception as e:
            self.log(infohash, f"Error adding torrent with metadata: {e}")

    def run(self):
        """The main event loop for the service."""
        self.log("SERVICE", "Running main loop...")
        while True:
            try:
                while not self.request_queue.empty():
                    req_type, payload = self.request_queue.get_nowait()

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
                            handle.piece_priority(piece_index, lt.top_priority)
                            self.log(infohash, f"Prioritized piece {piece_index}")

                alerts = self.ses.pop_alerts()
                for alert in alerts:
                    alert_type = type(alert).__name__
                    self.log("ALERT", f"Received alert: {alert_type} - {alert}")

                    if isinstance(alert, lt.piece_finished_alert):
                        h = alert.handle
                        infohash = str(h.info_hash()).upper()
                        piece_index = alert.piece_index
                        self.log(infohash, f"Finished downloading piece {piece_index}. Queueing for read.")
                        self.read_queue.append((infohash, piece_index))

                    elif isinstance(alert, lt.read_piece_alert):
                        h = alert.handle
                        if not alert.error:
                            infohash = str(h.info_hash()).upper()
                            piece_index = alert.piece_index
                            self.log(infohash, f"Successfully read piece {piece_index} from cache.")
                            key = ('piece', infohash, piece_index)
                            self.result_dict[key] = bytes(alert.data)
                        else:
                            self.log(str(h.info_hash()).upper(), f"Failed to read piece {alert.piece_index}: {alert.error}")

            except Exception as e:
                self.log("FATAL", f"Exception in main loop: {e}\n{traceback.format_exc()}")

            if self.read_queue:
                infohash, piece_index = self.read_queue.popleft()
                if infohash in self.handles:
                    handle = self.handles[infohash]
                    if handle.is_valid():
                        handle.read_piece(piece_index)

            time.sleep(0.1)
