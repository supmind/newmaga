import av
import os
import traceback

os.makedirs('screenshots', exist_ok=True)

def create_screenshots_from_stream(file_like_object, infohash: str, queue, num_screenshots: int = 20):
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

                # Seek to the keyframe before the target timestamp
                seek_target = int(timestamp_sec * av.time_base)
                container.seek(seek_target, backward=True, any_frame=False, stream=video_stream)

                # Decode frames and find the one closest to the target timestamp
                best_frame = None
                min_diff = float('inf')

                for frame in container.decode(video=0):
                    frame_timestamp_sec = frame.pts * frame.time_base
                    diff = abs(frame_timestamp_sec - timestamp_sec)

                    if diff < min_diff:
                        min_diff = diff
                        best_frame = frame

                    # Stop decoding if we have passed the target by a reasonable margin (e.g., 1 second)
                    # This is a heuristic to avoid decoding the entire rest of the file.
                    if frame_timestamp_sec > timestamp_sec + 1:
                        break

                if best_frame is None:
                    print(f"[{infohash}] Orchestrator: WARNING - Could not decode any frames near {timestamp_sec:.2f}s.")
                    continue

                frame = best_frame
                output_filename = f"screenshots/{infohash}_{int(frame.pts * frame.time_base)}.jpg"
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
