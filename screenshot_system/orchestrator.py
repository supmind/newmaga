import av
import os
import traceback

os.makedirs('screenshots', exist_ok=True)

def create_screenshots_from_stream(file_like_object, infohash: str, queue, num_screenshots: int = 20):
    container = None
    try:
        container = av.open(file_like_object, "r")

        if not container.streams.video:
            print(f"[{infohash}] No video streams found.")
            return

        video_stream = container.streams.video[0]

        if container.duration is None:
            print(f"[{infohash}] Could not determine duration.")
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

                print(f"Saved screenshot to {output_filename}")
                if queue:
                    queue.put(1)

            except StopIteration:
                break
            except Exception:
                pass

    except av.error.InvalidDataError:
        print(f"[{infohash}] Failed to open stream, file data may be corrupt.")
    except Exception as e:
        print(f"[{infohash}] FATAL orchestrator error: {e}")
    finally:
        if container:
            container.close()
