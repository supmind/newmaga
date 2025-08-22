import sys
from io import BytesIO
from pymp4.parser import Box
from screenshot_system.mock_downloader import MockDownloader

def walk_boxes(box, indent=0):
    """Recursively prints the box types and some content."""
    # The parser can yield non-box elements, so we need to be robust
    if not hasattr(box, 'type'):
        return

    box_type = box.type.decode('utf-8')
    print(f"{' ' * indent}- Type: {box_type}")

    # If we find the boxes we care about, print more info
    if box_type in ('stco', 'stts', 'stsc', 'stsz'):
        print(f"{' ' * (indent + 2)}... (Found important index box)")

    if hasattr(box, "boxes"):
        for b in box.boxes:
            walk_boxes(b, indent + 2)

def explore_mp4_atoms_with_mock():
    """
    Uses the MockDownloader to get a full MP4 file and parses it with pymp4.
    """
    downloader = MockDownloader()
    file_info = downloader.get_file_info()
    if not file_info:
        print("Explorer: Failed to load mock data.")
        return

    print("Explorer: Getting full file content from mock downloader...")
    full_video_data = downloader.download_byte_range("mock", 0, 0, file_info["size"])

    if not full_video_data:
        print("Explorer: Mock downloader returned no data.")
        return

    print("\n--- Parsing Full Mock Video Data ---")
    try:
        with BytesIO(full_video_data) as f:
            # pymp4's parse_stream expects a file-like object and yields boxes
            parsed_boxes = Box.parse_stream(f)
            print("Successfully started parsing stream. Walking the tree:")
            for box in parsed_boxes:
                walk_boxes(box)
    except Exception as e:
        print(f"Error parsing data: {e}")

if __name__ == "__main__":
    explore_mp4_atoms_with_mock()
