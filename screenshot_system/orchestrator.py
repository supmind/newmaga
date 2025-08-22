import av
import os
import sys
import io
from .downloader import Downloader
from .io_adapter import TorrentFileIO
from .video import get_video_duration # We can still use this for the initial check

# Ensure the output directory exists
os.makedirs('screenshots', exist_ok=True)

def is_streamable(downloader, infohash, file_index, file_size) -> bool:
    """
    Checks if an MP4 file is stream-optimized by checking for the moov atom
    in the first part of the file.
    """
    print("\nOrchestrator: Checking if file is streamable...")
    # Download the first 5MB. A moov atom is usually smaller than this.
    header_size = 5 * 1024 * 1024
    print(f"Orchestrator: Attempting to download header ({header_size} bytes)...")
    header_data = downloader.download_byte_range(infohash, file_index, 0, min(header_size, file_size))

    if not header_data or len(header_data) == 0:
        print("Orchestrator: Could not download header to check for streamability. Download returned no data.")
        return False

    # Use get_video_duration as a proxy for a valid moov atom in the header
    duration = get_video_duration(header_data)
    if duration > 0:
        print(f"Orchestrator: File is streamable. Duration: {duration:.2f}s")
        return True
    else:
        print("Orchestrator: File is not streamable (moov atom not found in header). Skipping.")
        return False


def create_screenshots_for_torrent(infohash: str, file_index: int, num_screenshots: int = 10):
    """
    Orchestrates the process of creating screenshots for a specific video file in a torrent.
    """
    downloader = Downloader()
    try:
        handle = downloader.get_torrent_handle(infohash)
        if not handle:
            print(f"Orchestrator: Could not get handle for {infohash}")
            return

        tor_info = handle.get_torrent_info()
        file_info = tor_info.file_at(file_index)
        file_size = file_info.size

        # 1. Check if the file is streamable (moov atom at the start)
        if not is_streamable(downloader, infohash, file_index, file_size):
            return

        # 2. If it is, proceed with the full process using the IO adapter
        print("Orchestrator: Proceeding with screenshot generation...")
        io_adapter = TorrentFileIO(downloader, infohash, file_index, file_size)

        with av.open(io_adapter, "r") as container:
            print("Orchestrator: Successfully opened torrent stream with PyAV via IO adapter.")
            duration_sec = container.duration / av.time_base

            for i in range(num_screenshots):
                timestamp_sec = (duration_sec / (num_screenshots + 1)) * (i + 1)

                try:
                    print(f"Orchestrator: Seeking to {timestamp_sec:.2f} seconds...")
                    container.seek(int(timestamp_sec * 1_000_000_000), unit='ns', backward=True, any_frame=False)

                    frame = next(container.decode(video=0))

                    output_filename = f"screenshots/{infohash}_{int(timestamp_sec)}.jpg"
                    frame.to_image().save(output_filename)
                    print(f"Orchestrator: Saved screenshot to {output_filename}")

                except Exception as e:
                    print(f"Orchestrator: Failed to generate screenshot at {timestamp_sec:.2f}s: {e}")

    except Exception as e:
        print(f"An error occurred in the orchestrator: {e}")
    finally:
        downloader.close_session()

if __name__ == '__main__':
    # Example of how to use the orchestrator.
    # A torrent infohash for a well-seeded, public domain movie.
    # e.g., Sintel trailer
    infohash = "08ada5a7a6183aae1e09d831df6748d566095a10"
    if len(sys.argv) > 1:
        infohash = sys.argv[1]
    else:
        print("Usage: python -m screenshot_system.orchestrator <infohash>")
        print(f"Using default infohash for Sintel trailer: {infohash}")

    file_index = 0 # Assuming the video is the first file.

    print(f"Orchestrator Test: Starting with infohash {infohash}")
    create_screenshots_for_torrent(infohash, file_index, num_screenshots=3)
    print("Orchestrator Test: Finished.")
