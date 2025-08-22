import av
import os
from .downloader import Downloader
from .io_adapter import TorrentFileIO
from .video import get_video_duration

# Ensure the output directory exists
os.makedirs('screenshots', exist_ok=True)

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

        # --- Get Duration ---
        # To get duration, we need the moov atom, which can be at the start or end.
        # First, try reading the header.
        print("Orchestrator: Fetching file header to find 'moov' atom...")
        header_size = 2 * 1024 * 1024 # 2MB
        header_data = downloader.download_byte_range(infohash, file_index, 0, header_size)

        duration = 0
        if header_data:
            duration = get_video_duration(header_data)

        # If duration not found in header, try the footer
        if duration <= 0:
            print("Orchestrator: 'moov' atom not in header, fetching footer...")
            footer_size = 2 * 1024 * 1024 # 2MB
            footer_offset = max(0, file_size - footer_size)
            # We need to download both header and footer for PyAV to parse it
            # as it might need to seek between them. This is a limitation
            # of trying to parse a file without having it all.
            # A better approach is to use the custom IO object.
            # Let's simplify and just pass the header and footer concatenated.
            # This is a heuristic and might not always work.
            footer_data = downloader.download_byte_range(infohash, file_index, footer_offset, footer_size)
            if footer_data:
                # PyAV is smart enough to find the moov atom in the buffer we give it.
                duration = get_video_duration(footer_data)

        if duration <= 0:
            print(f"Orchestrator: Could not determine video duration for {file_info.path}.")
            return

        print(f"Orchestrator: Video duration is {duration:.2f} seconds.")

        # --- Create Screenshots ---
        io_adapter = TorrentFileIO(downloader, infohash, file_index, file_size)

        with av.open(io_adapter) as container:
            print("Orchestrator: Successfully opened torrent stream with PyAV.")

            for i in range(num_screenshots):
                timestamp_sec = (duration / (num_screenshots + 1)) * (i + 1)

                try:
                    print(f"Orchestrator: Seeking to {timestamp_sec:.2f} seconds...")
                    # PyAV's seek is not guaranteed to be perfect on a non-seekable stream,
                    # but it will do its best to find the nearest keyframe.
                    container.seek(int(timestamp_sec * 1000000), unit='ns')

                    frame = next(container.decode(video=0))

                    output_filename = f"screenshots/{infohash}_{int(timestamp_sec)}.jpg"
                    frame.to_image().save(output_filename)
                    print(f"Orchestrator: Saved screenshot to {output_filename}")

                except Exception as e:
                    print(f"Orchestrator: Failed to generate screenshot at {timestamp_sec:.2f}s: {e}")

    finally:
        downloader.close_session()

if __name__ == '__main__':
    # Test the orchestrator with a known torrent
    # infohash = "EA36231CF154B033FFE0694F1FAAD8C8D97B9EEC" # 異世界おじさんちゃんねる
    # Let's use a torrent that is more likely to have video content that can be parsed
    infohash = "3F76E26318CBDD46F34D6925DBAE659BB403F222" # 南极大冒险
    file_index = 0 # Assuming the largest file is the video

    # We need to find the correct file index. Let's assume it's the largest file.
    # A real implementation would get this from the classifier step.

    # A quick way to get the file index without a full torrent client
    # is to assume the largest file is the one we want.
    # This logic should eventually live in the main example.py

    print("Orchestrator Test: Starting...")
    create_screenshots_for_torrent(infohash, file_index, num_screenshots=3) # Just 3 for testing
    print("Orchestrator Test: Finished.")
