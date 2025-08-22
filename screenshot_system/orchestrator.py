import av
import os
import sys
from .downloader import Downloader
from .io_adapter import TorrentFileIO

# Ensure the output directory exists
os.makedirs('screenshots', exist_ok=True)

def create_screenshots_for_torrent(infohash: str, file_index: int, num_screenshots: int = 10):
    """
    Orchestrates the process of creating screenshots for a specific video file in a torrent.
    """
    downloader = Downloader()
    io_adapter = None
    try:
        # Get torrent handle and file info first
        handle = downloader.get_torrent_handle(infohash)
        if not handle:
            print(f"Orchestrator: Could not get handle for {infohash}")
            return

        tor_info = handle.get_torrent_info()
        file_info = tor_info.file_at(file_index)
        file_size = file_info.size

        # Create the custom IO adapter
        io_adapter = TorrentFileIO(downloader, infohash, file_index, file_size)

        # Open the torrent file via our custom IO adapter
        with av.open(io_adapter, "r") as container:
            print("Orchestrator: Successfully opened torrent stream with PyAV.")

            # Get duration
            if container.duration is None:
                print(f"Orchestrator: Could not determine video duration for {file_info.path}.")
                return
            duration_sec = container.duration / av.time_base
            print(f"Orchestrator: Video duration is {duration_sec:.2f} seconds.")

            # --- Create Screenshots ---
            for i in range(num_screenshots):
                timestamp_sec = (duration_sec / (num_screenshots + 1)) * (i + 1)

                try:
                    print(f"Orchestrator: Seeking to {timestamp_sec:.2f} seconds...")
                    # The 'ns' unit is for nanoseconds
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
        # The IO adapter doesn't need closing, but the downloader session does.
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
