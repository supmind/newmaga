import av
import sys

def take_midpoint_screenshot(video_path):
    """
    Opens a video file, seeks to the middle, and saves a screenshot.
    """
    try:
        with av.open(video_path) as container:
            # Check if there is a video stream
            video_stream = next((s for s in container.streams if s.type == 'video'), None)
            if video_stream is None:
                print("No video stream found in the file.")
                return

            # Calculate the midpoint timestamp
            duration = container.duration
            if not duration:
                print("Could not determine video duration.")
                return

            midpoint_timestamp = duration // 2

            # Seek to the midpoint. The seek is to the nearest keyframe before the timestamp.
            # Seek to the keyframe before the midpoint
            container.seek(midpoint_timestamp, backward=True, any_frame=False, stream=video_stream)

            # Decode frames to find the one closest to the midpoint
            best_frame = None
            min_diff = float('inf')
            midpoint_sec = midpoint_timestamp / av.time_base

            for frame in container.decode(video=video_stream):
                frame_sec = frame.pts * frame.time_base
                diff = abs(frame_sec - midpoint_sec)
                if diff < min_diff:
                    min_diff = diff
                    best_frame = frame

                if frame_sec > midpoint_sec + 1: # Stop 1s after midpoint
                    break

            if best_frame:
                print(f"Successfully decoded a frame at timestamp {best_frame.pts * best_frame.time_base:.2f}s")
                output_filename = "screenshot.jpg"
                best_frame.to_image().save(output_filename)
                print(f"Screenshot saved to {output_filename}")
            else:
                print("Could not decode a suitable frame.")

    except av.AVError as e:
        print(f"An error occurred with PyAV: {e}")
    except FileNotFoundError:
        print(f"Error: The file '{video_path}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pyav_example.py <path_to_video_file>")
        sys.exit(1)

    video_file = sys.argv[1]
    print(f"Attempting to take a screenshot from '{video_file}'...")
    take_midpoint_screenshot(video_file)
