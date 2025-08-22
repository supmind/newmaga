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
        container = av.open(file_like_object, "r")

        if not container.streams.video:
            return

        video_stream = container.streams.video[0]

        if container.duration is None:
            return

        duration_sec = container.duration / av.time_base

        for i in range(num_screenshots):
            timestamp_sec = (duration_sec / (num_screenshots + 1)) * (i + 1)
            try:
                seek_target = int(timestamp_sec * av.time_base)
                container.seek(seek_target, backward=True, any_frame=False, stream=video_stream)

                frame = next(container.decode(video=0))

                output_filename = f"screenshots/{infohash}_{int(timestamp_sec)}.jpg"
                frame.to_image().save(output_filename)

                print(f"Orchestrator: Saved screenshot to {output_filename}")
                if queue:
                    queue.put(1)

            except StopIteration:
                break
            except Exception:
                # We are silencing all screenshot-specific errors as requested.
                # The main process will only show the final summary.
                pass

    except av.error.InvalidDataError:
        # Silencing corrupt file errors
        pass
    except Exception:
        # Silencing all other fatal errors in the orchestrator
        pass
    finally:
        if container:
            container.close()
