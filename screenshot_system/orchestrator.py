import av
import os
from .video import get_video_duration

os.makedirs('screenshots', exist_ok=True)

def create_screenshots_from_stream(file_like_object, num_screenshots: int = 10, mock_mode=False):
    """
    Generates screenshots from a given file-like object that PyAV can read.

    :param file_like_object: A readable, seekable file-like object.
    :param num_screenshots: The number of screenshots to generate.
    :param mock_mode: If true, uses a generic prefix for screenshot names.
    """
    print("Orchestrator: Proceeding with screenshot generation from stream...")
    try:
        # The TorrentFileIO object acts as a file-like object for PyAV.
        # When PyAV seeks and reads to decode a frame, it calls the `read()` method
        # on the IO adapter, which in turn calls our downloader to fetch
        # the required byte range from the torrent on-demand.
        with av.open(file_like_object, "r") as container:
            print("Orchestrator: Successfully opened stream with PyAV.")
            # Some streams might not have a duration, handle this gracefully.
            if container.duration is None:
                print("Orchestrator: ERROR: Could not determine stream duration.")
                return

            duration_sec = container.duration / av.time_base

            for i in range(num_screenshots):
                timestamp_sec = (duration_sec / (num_screenshots + 1)) * (i + 1)
                try:
                    print(f"Orchestrator: Seeking to {timestamp_sec:.2f} seconds...")
                    # We seek to the timestamp in the container's time_base.
                    seek_target = int(timestamp_sec * av.time_base)
                    container.seek(seek_target, backward=True, any_frame=True)

                    frame = next(container.decode(video=0))

                    prefix = "mock" if mock_mode else "screenshot"
                    output_filename = f"screenshots/{prefix}_{int(timestamp_sec)}.jpg"
                    frame.to_image().save(output_filename)
                    print(f"Orchestrator: Saved screenshot to {output_filename}")
                except StopIteration:
                    print(f"Orchestrator: Could not decode frame at {timestamp_sec:.2f}s. Reached end of stream?")
                except Exception as e:
                    print(f"Orchestrator: Failed to generate screenshot at {timestamp_sec:.2f}s: {e}")

    except Exception as e:
        print(f"An error occurred in the orchestrator: {e}")

# The main entry point for this system is now `example.py`.
# This module is intended to be used as a library.
