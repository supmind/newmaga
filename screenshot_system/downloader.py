import libtorrent as lt
import time
import sys

class Downloader:
    def __init__(self):
        settings = {
            'listen_interfaces': '0.0.0.0:6881',
            'alert_mask': lt.alert_category.storage | lt.alert_category.status,
            'enable_dht': True
        }
        self.ses = lt.session(settings)
        self.ses.add_dht_router("router.utorrent.com", 6881)
        self.ses.add_dht_router("router.bittorrent.com", 6881)
        self.ses.add_dht_router("dht.transmissionbt.com", 6881)
        self.handles = {}

    def get_torrent_handle(self, infohash: str):
        if infohash in self.handles:
            return self.handles[infohash]

        magnet_link = f"magnet:?xt=urn:btih:{infohash}"
        params = {'save_path': '/tmp/', 'storage_mode': lt.storage_mode_t(2)}
        handle = lt.add_magnet_uri(self.ses, magnet_link, params)
        self.handles[infohash] = handle

        print("Downloading metadata...")
        start_time = time.time()
        while not handle.has_metadata():
            self._wait_for_alert()
            if time.time() - start_time > 30: # 30 second timeout for metadata
                print("Timeout waiting for metadata.")
                self.ses.remove_torrent(handle)
                del self.handles[infohash]
                return None
        print("Metadata downloaded.")

        handle.prioritize_files([0] * handle.get_torrent_info().num_files())
        return handle

    def _wait_for_alert(self, timeout=1.0):
        self.ses.wait_for_alert(int(timeout * 1000))
        return self.ses.pop_alerts()

    def download_byte_range(self, infohash: str, file_index: int, offset: int, size: int) -> bytes:
        handle = self.get_torrent_handle(infohash)
        if not handle:
            return b''

        tor_info = handle.get_torrent_info()
        piece_size = tor_info.piece_length()

        # file_at() is on torrent_info, not the handle
        file_info = tor_info.file_at(file_index)

        # Calculate piece range based on the file's offset in the torrent
        file_offset = file_info.offset
        abs_offset = file_offset + offset

        start_piece, start_piece_offset = divmod(abs_offset, piece_size)
        end_piece, _ = divmod(abs_offset + size - 1, piece_size)

        print(f"Requesting pieces from {start_piece} to {end_piece} for file {file_index}")

        for i in range(start_piece, end_piece + 1):
            handle.read_piece(i)

        downloaded_data = {}
        expected_pieces = set(range(start_piece, end_piece + 1))

        start_time = time.time()
        while expected_pieces:
            if time.time() - start_time > 60: # 60 second timeout for pieces
                print("Timeout waiting for pieces.")
                return b''

            alerts = self._wait_for_alert()
            for alert in alerts:
                if isinstance(alert, lt.read_piece_alert):
                    if alert.piece in expected_pieces:
                        print(f"Received piece {alert.piece}")
                        downloaded_data[alert.piece] = alert.buffer
                        expected_pieces.remove(alert.piece)

        print("All required pieces received.")

        # Debugging: check the size of each piece
        for piece_index, piece_data in downloaded_data.items():
            print(f"  - Debug: Piece {piece_index} has size {len(piece_data)}")

        full_chunk = b"".join(downloaded_data[i] for i in sorted(downloaded_data))
        print(f"  - Debug: Total combined chunk size is {len(full_chunk)}")

        # Slice the final result
        # The start of our desired data relative to the start of the downloaded chunk
        slice_start = abs_offset - (start_piece * piece_size)
        slice_end = slice_start + size

        result = full_chunk[slice_start:slice_end]
        print(f"  - Debug: Slicing from {slice_start} to {slice_end}. Result size: {len(result)}")

        return result

    def close_session(self):
        for infohash, handle in list(self.handles.items()):
            self.ses.remove_torrent(handle)
        self.handles = {}

if __name__ == '__main__':
    downloader = Downloader()
    infohash = "EA36231CF154B033FFE0694F1FAAD8C8D97B9EEC"

    handle = downloader.get_torrent_handle(infohash)
    if handle:
        tor_info = handle.get_torrent_info()
        file_index = 0
        file_info = tor_info.file_at(file_index)

        offset = 0
        size = 1024

        print(f"\nAttempting to download {size} bytes from file '{file_info.path}' at offset {offset}")
        data = downloader.download_byte_range(infohash, file_index, offset, size)

        if data:
            print(f"Successfully downloaded {len(data)} bytes.")
        else:
            print("Download failed or returned no data.")

    downloader.close_session()
