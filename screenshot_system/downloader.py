import libtorrent as lt
import time
import sys

class Downloader:
    def __init__(self):
        # Apply libtorrent's high-performance seed settings preset
        settings = lt.high_performance_seed()
        # Customize the preset
        settings['listen_interfaces'] = '0.0.0.0:0'
        settings['alert_mask'] = lt.alert_category.status | lt.alert_category.storage
        settings['connections_limit'] = 200

        self.ses = lt.session(settings)
        # Add more DHT routers for faster peer discovery
        self.ses.add_dht_router("router.utorrent.com", 6881)
        self.ses.add_dht_router("router.bittorrent.com", 6881)
        self.ses.add_dht_router("dht.transmissionbt.com", 6881)
        self.ses.add_dht_router("dht.aelitis.com", 6881)
        self.ses.add_dht_router("router.bitcomet.com", 6881)
        self.handles = {}

    def get_torrent_handle(self, infohash: str):
        if infohash in self.handles:
            return self.handles[infohash]

        magnet_link = f"magnet:?xt=urn:btih:{infohash}"
        params = {'save_path': '/tmp/', 'storage_mode': lt.storage_mode_t(2)}
        handle = lt.add_magnet_uri(self.ses, magnet_link, params)
        self.handles[infohash] = handle

        start_time = time.time()
        while not handle.has_metadata():
            self._wait_for_alert()
            if time.time() - start_time > 30:
                self.ses.remove_torrent(handle)
                del self.handles[infohash]
                return None

        torrent_file = handle.torrent_file()
        if torrent_file:
            handle.prioritize_files([0] * torrent_file.num_files())
        return handle

    def _wait_for_alert(self, timeout=1.0):
        self.ses.wait_for_alert(int(timeout * 1000))
        return self.ses.pop_alerts()

    def download_byte_range(self, infohash: str, file_index: int, offset: int, size: int) -> bytes:
        handle = self.get_torrent_handle(infohash)
        if not handle:
            return b''

        torrent_file = handle.torrent_file()
        if not torrent_file:
            return b''

        files = torrent_file.files()
        piece_size = torrent_file.piece_length()
        file_offset = files.file_offset(file_index)
        abs_offset = file_offset + offset

        start_piece, start_piece_offset = divmod(abs_offset, piece_size)
        end_piece, _ = divmod(abs_offset + size - 1, piece_size)

        for i in range(start_piece, end_piece + 1):
            handle.piece_priority(i, 7)

        expected_pieces_to_finish = set(range(start_piece, end_piece + 1))
        start_time = time.time()
        last_status_print = 0
        while expected_pieces_to_finish:
            if time.time() - start_time > 1800: # 30 minute timeout for downloading
                return b''

            if time.time() - last_status_print > 2:
                s = handle.status()
                if s.state in [4, 5]: # 4 is 'finished', 5 is 'seeding'
                    break
                last_status_print = time.time()

            alerts = self._wait_for_alert(timeout=0.5)
            for alert in alerts:
                if isinstance(alert, lt.piece_finished_alert):
                    if alert.piece_index in expected_pieces_to_finish:
                        expected_pieces_to_finish.remove(alert.piece_index)

        for i in range(start_piece, end_piece + 1):
            handle.read_piece(i)

        downloaded_data = {}
        expected_pieces_to_read = set(range(start_piece, end_piece + 1))
        start_time = time.time()
        while expected_pieces_to_read:
            if time.time() - start_time > 120: # 2 minute timeout for reading
                return b''
            alerts = self._wait_for_alert()
            for alert in alerts:
                if isinstance(alert, lt.read_piece_alert):
                    if alert.piece in expected_pieces_to_read:
                        downloaded_data[alert.piece] = bytes(alert.buffer)
                        expected_pieces_to_read.remove(alert.piece)

        for i in range(start_piece, end_piece + 1):
            handle.piece_priority(i, 1)

        if len(downloaded_data) != (end_piece - start_piece + 1):
            return b''

        full_chunk = b"".join(downloaded_data[i] for i in sorted(downloaded_data))
        slice_start = abs_offset - (start_piece * piece_size)
        slice_end = slice_start + size
        return full_chunk[slice_start:slice_end]

    def close_session(self):
        for infohash, handle in list(self.handles.items()):
            try:
                self.ses.remove_torrent(handle)
            except lt.error:
                pass
        self.handles = {}

if __name__ == '__main__':
    downloader = Downloader()
    infohash = "08ada5a7a6183aae1e09d831df6748d566095a10" # Sintel (streamable)
    handle = downloader.get_torrent_handle(infohash)
    if handle:
        torrent_file = handle.torrent_file()
        if torrent_file:
            files = torrent_file.files()
            file_index = 0
            file_path = files.file_path(file_index)
            offset = 10 * 1024 * 1024
            size = 1024
            print(f"\nAttempting to download {size} bytes from file '{file_path}' at offset {offset}")
            data = downloader.download_byte_range(infohash, file_index, offset, size)
            if data:
                print(f"Successfully downloaded {len(data)} bytes.")
            else:
                print("Download failed or returned no data.")
        else:
            print("Could not get torrent file info.")
    downloader.close_session()
