import libtorrent as lt
import time
import sys

class Downloader:
    def __init__(self):
        settings = lt.settings_pack()
        settings.set_str("listen_interfaces", "0.0.0.0:0")
        settings.set_int("alert_mask",
            lt.alert_category.status |
            lt.alert_category.storage |
            lt.alert_category.error
        )
        settings.set_int("connections_limit", 200)

        self.ses = lt.session(settings)
        self.ses.add_dht_router("router.utorrent.com", 6881)
        self.ses.add_dht_router("router.bittorrent.com", 6881)
        self.ses.add_dht_router("dht.transmissionbt.com", 6881)
        self.ses.add_dht_router("dht.aelitis.com", 6881)
        self.ses.add_dht_router("router.bitcomet.com", 6881)
        self.handles = {}

    def get_torrent_handle(self, infohash: str):
        if infohash in self.handles:
            return self.handles[infohash]

        params = {
            'info_hashes': infohash,
            'save_path': '/tmp/',
            'storage_mode': lt.storage_mode_t.storage_mode_sparse
        }
        handle = self.ses.add_torrent(params)
        self.handles[infohash] = handle

        meta_start_time = time.time()
        while not handle.status().has_metadata:
            self._wait_for_alert()
            if time.time() - meta_start_time > 60:
                print(f"Timeout getting metadata for {infohash}")
                if handle.is_valid():
                    self.ses.remove_torrent(handle, lt.session.delete_files)
                del self.handles[infohash]
                return None
        return handle

    def _wait_for_alert(self, timeout=1.0):
        self.ses.wait_for_alerts(int(timeout * 1000))
        return self.ses.pop_alerts()

    def download_byte_range(self, infohash: str, file_index: int, offset: int, size: int) -> bytes:
        handle = self.get_torrent_handle(infohash)
        if not handle:
            return b''

        ti = handle.get_torrent_info()
        if not ti:
            return b''

        piece_size = ti.piece_length()
        fs = ti.files()
        file_offset = fs.file_offset(file_index)
        abs_offset = file_offset + offset

        start_piece, _ = divmod(abs_offset, piece_size)
        end_piece, _ = divmod(abs_offset + size - 1, piece_size)

        pieces_needed = set(range(start_piece, end_piece + 1))

        for p_idx in pieces_needed:
            handle.piece_priority(p_idx, lt.top_priority)

        download_start_time = time.time()
        pieces_done = set()
        while pieces_done != pieces_needed:
            if time.time() - download_start_time > 1800: # 30 min timeout
                return b''

            s = handle.status()
            if s.state in [lt.torrent_status.states.finished, lt.torrent_status.states.seeding]:
                break

            alerts = self.ses.pop_alerts()
            for alert in alerts:
                if isinstance(alert, lt.piece_finished_alert):
                    if alert.piece_index in pieces_needed:
                        pieces_done.add(alert.piece_index)

        all_data = {}
        for piece_index in sorted(list(pieces_needed)):
            handle.read_piece(piece_index)
            self.ses.wait_for_alerts(10000)
            alerts = self.ses.pop_alerts()
            found_piece = False
            for alert in alerts:
                if isinstance(alert, lt.read_piece_alert) and alert.piece_index == piece_index:
                    all_data[piece_index] = bytes(alert.data)
                    found_piece = True
                    break
            if not found_piece:
                return b'' # Failed to read

        full_chunk = b"".join(all_data[i] for i in sorted(all_data))
        p_offset = start_piece * piece_size
        start = abs_offset - p_offset
        end = start + size
        return full_chunk[start:end]

    def close_session(self):
        for infohash, handle in list(self.handles.items()):
            try:
                if handle.is_valid():
                    self.ses.remove_torrent(handle, lt.session.delete_files)
            except lt.libtorrent_error:
                pass
        self.handles = {}
