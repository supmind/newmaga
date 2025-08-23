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
            # We use 'ns' unit for high precision.
            container.seek(midpoint_timestamp, unit='ns', backward=True, any_frame=False, stream=video_stream)

            # Decode frames until we get one at or after our target
            for frame in container.decode(video=video_stream):
                # We got the first frame after the seek, which is what we want.
                print(f"Successfully decoded a frame at timestamp {frame.pts * video_stream.time_base:.2f}s")

                # Save the frame to an image file
                output_filename = "screenshot.jpg"
                frame.to_image().save(output_filename)
                print(f"Screenshot saved to {output_filename}")
                return # We only need one screenshot

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
