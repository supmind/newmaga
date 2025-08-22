import av
import os
import sys
import io
from .downloader import Downloader
from .io_adapter import TorrentFileIO
from .video import get_video_duration

os.makedirs('screenshots', exist_ok=True)

def is_streamable(downloader, infohash, file_index, file_size) -> bool:
    # This function is problematic for mocks and real scenarios.
    # The real downloader is needed to get metadata, but for streaming, we might use a mock.
    # For now, we'll assume the check is part of the orchestrator's role.
    print("\nOrchestrator: Checking if file is streamable...")
    header_size = 1 * 1024 * 1024  # 1MB is enough for headers
    print(f"Orchestrator: Attempting to download header ({header_size} bytes)...")
    header_data = downloader.download_byte_range(infohash, file_index, 0, min(header_size, file_size))

    if not header_data or len(header_data) == 0:
        print("Orchestrator: Could not download header to check for streamability.")
        return False

    duration = get_video_duration(header_data)
    if duration > 0:
        print(f"Orchestrator: File is streamable. Duration: {duration:.2f}s")
        return True
    else:
        print("Orchestrator: File is not streamable (moov atom not found in header). Skipping.")
        return False

def create_screenshots_for_torrent(infohash: str, target_path: str, num_screenshots: int = 10, downloader=None):
    close_downloader_on_exit = False
    if downloader is None:
        downloader = Downloader()
        close_downloader_on_exit = True

    try:
        target_file_index = -1
        file_size = -1

        if not hasattr(downloader, 'get_torrent_handle'):  # Mock downloader branch
            print("Orchestrator: Using mock downloader. Bypassing torrent metadata lookup.")
            mock_info = downloader.get_file_info()
            if not mock_info:
                print("Orchestrator: Mock downloader failed to provide file info.")
                return
            target_file_index = 0  # Mock only has one file
            file_size = mock_info['size']
        else:  # Real downloader branch
            handle = downloader.get_torrent_handle(infohash)
            if not handle:
                print(f"Orchestrator: Could not get handle for {infohash}")
                return
            tor_info = handle.get_torrent_info()
            file_info = None
            for i in range(tor_info.num_files()):
                f = tor_info.file_at(i)
                if target_path == f.path:
                    target_file_index = i
                    file_info = f
                    break
            if target_file_index == -1:
                print(f"Orchestrator: Could not find file '{target_path}' in torrent {infohash}")
                return
            file_size = file_info.size

        if not is_streamable(downloader, infohash, target_file_index, file_size):
            # For a mock downloader, this check might be less meaningful but we keep it for API consistency
            return

        print("Orchestrator: Proceeding with screenshot generation...")
        # The TorrentFileIO object acts as a file-like object for PyAV.
        # When PyAV seeks and reads to decode a frame, it calls the `read()` method
        # on the IO adapter, which in turn calls our downloader to fetch
        # the required byte range from the torrent on-demand.
        io_adapter = TorrentFileIO(downloader, infohash, target_file_index, file_size)

        with av.open(io_adapter, "r") as container:
            print("Orchestrator: Successfully opened torrent stream with PyAV via IO adapter.")
            duration_sec = container.duration / av.time_base

            for i in range(num_screenshots):
                timestamp_sec = (duration_sec / (num_screenshots + 1)) * (i + 1)
                try:
                    print(f"Orchestrator: Seeking to {timestamp_sec:.2f} seconds...")
                    # When using custom IO, seek might not support all arguments.
                    # We seek to the timestamp in the container's time_base.
                    seek_target = int(timestamp_sec * av.time_base)
                    container.seek(seek_target, backward=True, any_frame=True)
                    frame = next(container.decode(video=0))
                    # Use a more descriptive filename for mock tests
                    mock_prefix = "mock" if hasattr(downloader, 'video_path') else infohash
                    output_filename = f"screenshots/{mock_prefix}_{int(timestamp_sec)}.jpg"
                    frame.to_image().save(output_filename)
                    print(f"Orchestrator: Saved screenshot to {output_filename}")
                except StopIteration:
                    print(f"Orchestrator: Could not decode frame at {timestamp_sec:.2f}s. Reached end of stream?")
                except Exception as e:
                    print(f"Orchestrator: Failed to generate screenshot at {timestamp_sec:.2f}s: {e}")

    except Exception as e:
        print(f"An error occurred in the orchestrator: {e}")
    finally:
        if close_downloader_on_exit and downloader:
            downloader.close_session()

if __name__ == '__main__':
    infohash = "08ada5a7a6183aae1e09d831df6748d566095a10"
    if len(sys.argv) > 1:
        infohash = sys.argv[1]
    else:
        print("Usage: python -m screenshot_system.orchestrator <infohash> [file_path]")
        print(f"Using default infohash for Sintel trailer: {infohash}")

    # This test block is now simplified as we don't know the file path without metadata
    # The main entry point is now example.py
    print(f"Orchestrator module can be tested via example.py")
