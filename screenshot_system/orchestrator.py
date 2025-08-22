import av
import os
import traceback

os.makedirs('screenshots', exist_ok=True)

def create_screenshots_from_stream(file_like_object, infohash: str, queue, num_screenshots: int = 20):
    """
    Generates screenshots from a given file-like object that PyAV can read.
    """
    container = None
    try:
        print(f"[{infohash}] Orchestrator: Opening stream with PyAV...")
        container = av.open(file_like_object, "r")
        print(f"[{infohash}] Orchestrator: Stream opened successfully.")

        if not container.streams.video:
            print(f"[{infohash}] Orchestrator: ERROR - No video streams found.")
            return

        video_stream = container.streams.video[0]

        if container.duration is None:
            print(f"[{infohash}] Orchestrator: ERROR - Could not determine duration.")
            return

        duration_sec = container.duration / av.time_base
        print(f"[{infohash}] Orchestrator: Video duration is {duration_sec:.2f}s.")

        for i in range(num_screenshots):
            timestamp_sec = (duration_sec / (num_screenshots + 1)) * (i + 1)
            try:
                print(f"[{infohash}] Orchestrator: Processing screenshot at {timestamp_sec:.2f}s...")

                seek_target = int(timestamp_sec * av.time_base)
                container.seek(seek_target, backward=True, any_frame=False, stream=video_stream)

                frame = next(container.decode(video=0))

                output_filename = f"screenshots/{infohash}_{int(timestamp_sec)}.jpg"
                frame.to_image().save(output_filename)

                print(f"Orchestrator: Saved screenshot to {output_filename}")
                if queue:
                    queue.put(1)

            except StopIteration:
                print(f"[{infohash}] Orchestrator: WARNING - Could not decode frame at {timestamp_sec:.2f}s (end of stream?).")
                break
            except Exception as e:
                print(f"[{infohash}] Orchestrator: ERROR - Failed to generate screenshot at {timestamp_sec:.2f}s: {e}")

    except av.error.InvalidDataError:
        print(f"[{infohash}] Orchestrator: ERROR - Failed to open stream, file data may be corrupt or invalid.")
    except Exception as e:
        print(f"[{infohash}] Orchestrator: FATAL - An unexpected error occurred: {e}")
        traceback.print_exc()
    finally:
        if container:
            container.close()
