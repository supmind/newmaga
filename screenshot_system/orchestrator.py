import av
import os
import traceback
from .video import get_video_duration

os.makedirs('screenshots', exist_ok=True)

def create_screenshots_from_stream(file_like_object, num_screenshots: int = 20, mock_mode=False):
    """
    Generates screenshots from a given file-like object that PyAV can read.
    """
    container = None
    try:
        print("Orchestrator: Attempting to open stream with PyAV...")
        container = av.open(file_like_object, "r")
        print("Orchestrator: Successfully opened stream with PyAV.")

        # Check if there are any video streams available.
        if not container.streams.video:
            print("Orchestrator: ERROR: No video streams found in the file.")
            return

        video_stream = container.streams.video[0]

        # Some streams might not have a duration, handle this gracefully.
        if container.duration is None:
            print("Orchestrator: ERROR: Could not determine stream duration.")
            return

        duration_sec = container.duration / av.time_base

        for i in range(num_screenshots):
            timestamp_sec = (duration_sec / (num_screenshots + 1)) * (i + 1)
            try:
                print(f"Orchestrator: Processing timestamp {timestamp_sec:.2f}s...")

                # We seek to the timestamp in the container's time_base.
                seek_target = int(timestamp_sec * av.time_base)

                print(f"  - Seeking to {seek_target}...")
                container.seek(seek_target, backward=True, any_frame=False, stream=video_stream)

                print(f"  - Decoding frame...")
                frame = next(container.decode(video=0))

                print(f"  - Saving frame to image...")
                prefix = "mock" if mock_mode else "screenshot"
                output_filename = f"screenshots/{prefix}_{int(timestamp_sec)}.jpg"
                frame.to_image().save(output_filename)

                print(f"Orchestrator: Saved screenshot to {output_filename}")

            except StopIteration:
                print(f"Orchestrator: WARNING: Could not decode frame at {timestamp_sec:.2f}s. Reached end of stream?")
                break # No more frames to decode
            except Exception as e:
                print(f"Orchestrator: ERROR: Failed to generate screenshot at {timestamp_sec:.2f}s.")
                print("--- DETAILED ERROR ---")
                traceback.print_exc()
                print("----------------------")

    except Exception as e:
        print("Orchestrator: FATAL: An unexpected error occurred in the orchestrator.")
        print("--- DETAILED ERROR ---")
        traceback.print_exc()
        print("----------------------")
    finally:
        if container:
            container.close()
