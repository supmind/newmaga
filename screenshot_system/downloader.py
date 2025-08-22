import libtorrent as lt
import time
import sys

class Downloader:
    def __init__(self):
        settings = {
            # Set port to 0 to automatically pick a random available port.
            # This is CRITICAL for running multiple downloaders in parallel to avoid port conflicts.
            'listen_interfaces': '0.0.0.0:0',
            'alert_mask': lt.alert_category.status | lt.alert_category.storage,
            'enable_dht': True,
            # Performance tuning: allow more connections
            'connections_limit': 200,
        }
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

        print("Downloading metadata...")
        start_time = time.time()
        while not handle.has_metadata():
            self._wait_for_alert()
            if time.time() - start_time > 30:
                print("Timeout waiting for metadata.")
                self.ses.remove_torrent(handle)
                del self.handles[infohash]
                return None
        print("Metadata downloaded.")

        # Prioritize no files by default.
        # Use modern API torrent_file() instead of deprecated get_torrent_info()
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
            print("Downloader: Could not get torrent_file from handle.")
            return b''

        files = torrent_file.files()
        piece_size = torrent_file.piece_length()
        file_offset = files.file_offset(file_index)
        abs_offset = file_offset + offset

        start_piece, start_piece_offset = divmod(abs_offset, piece_size)
        end_piece, _ = divmod(abs_offset + size - 1, piece_size)

        print(f"Requesting pieces from {start_piece} to {end_piece} for file {file_index}")

        # Phase 1: Prioritize and wait for pieces to be downloaded
        for i in range(start_piece, end_piece + 1):
            handle.piece_priority(i, 7)

        expected_pieces_to_finish = set(range(start_piece, end_piece + 1))
        start_time = time.time()
        last_status_print = 0
        while expected_pieces_to_finish:
            if time.time() - start_time > 1800: # 30 minute timeout for downloading
                print("\nTimeout waiting for pieces to download.")
                return b''

            # Print status every 2 seconds
            if time.time() - last_status_print > 2:
                s = handle.status()
                # This state list is based on libtorrent 1.2.x/2.0.x enum `torrent_status::state_t`
                state_str = [
                    'queued_for_checking', 'checking_files', 'downloading_metadata', 'downloading',
                    'finished', 'seeding', 'allocating', 'checking_resume_data'
                ]
                state_index = s.state
                if state_index < len(state_str):
                    state = state_str[state_index]
                else:
                    state = f'unknown ({state_index})'

                status_line = f"  - Status: {state} | Progress: {s.progress * 100:.2f}% | Peers: {s.num_peers} | Down: {s.download_rate / 1000:.1f} kB/s"
                print(status_line, end='\r')
                last_status_print = time.time()

                # state_index 4 is 'finished', 5 is 'seeding'. If libtorrent reports these states,
                # we can assume the download is done even if we missed the piece_finished_alert.
                if state_index in [4, 5]:
                    print(f"\nDetected '{state}' status, breaking download wait loop.")
                    break

            alerts = self._wait_for_alert(timeout=0.5)
            for alert in alerts:
                if isinstance(alert, lt.piece_finished_alert):
                    if alert.piece_index in expected_pieces_to_finish:
                        print(f"\nEvent: Finished downloading piece {alert.piece_index}")
                        expected_pieces_to_finish.remove(alert.piece_index)

        print("All required pieces finished downloading.")

        # Phase 2: Now that pieces are downloaded, read them
        for i in range(start_piece, end_piece + 1):
            handle.read_piece(i)

        downloaded_data = {}
        expected_pieces_to_read = set(range(start_piece, end_piece + 1))
        start_time = time.time()
        while expected_pieces_to_read:
            if time.time() - start_time > 120: # 2 minute timeout for reading
                print("Timeout waiting for piece data from storage.")
                return b''
            alerts = self._wait_for_alert()
            for alert in alerts:
                if isinstance(alert, lt.read_piece_alert):
                    if alert.piece in expected_pieces_to_read:
                        print(f"Read data for piece {alert.piece} (size: {len(alert.buffer)})")
                        downloaded_data[alert.piece] = bytes(alert.buffer)
                        expected_pieces_to_read.remove(alert.piece)

        # De-prioritize pieces
        for i in range(start_piece, end_piece + 1):
            handle.piece_priority(i, 1)

        if len(downloaded_data) != (end_piece - start_piece + 1):
            print("Error: Did not receive all requested piece data.")
            return b''

        # Assemble and slice
        full_chunk = b"".join(downloaded_data[i] for i in sorted(downloaded_data))
        slice_start = abs_offset - (start_piece * piece_size)
        slice_end = slice_start + size
        return full_chunk[slice_start:slice_end]

    def close_session(self):
        # ... (rest of the class is the same)
        for infohash, handle in list(self.handles.items()):
            self.ses.remove_torrent(handle)
        self.handles = {}

if __name__ == '__main__':
    # This block is for basic testing of the downloader itself.
    downloader = Downloader()
    infohash = "08ada5a7a6183aae1e09d831df6748d566095a10" # Sintel (streamable)

    handle = downloader.get_torrent_handle(infohash)
    if handle:
        torrent_file = handle.torrent_file()
        if torrent_file:
            files = torrent_file.files()
            file_index = 0 # Assuming we want the first file
            file_path = files.file_path(file_index)

            offset = 10 * 1024 * 1024 # 10MB into the file
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
