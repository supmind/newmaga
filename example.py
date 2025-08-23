import multiprocessing
import os
import time

# We must import libtorrent here to check the version in this process
try:
    import libtorrent as lt
    MAIN_LT_VERSION = lt.version
except Exception as e:
    MAIN_LT_VERSION = f"Error importing or getting version in main process: {e}"


def downloader_service_bootstrap():
    """
    This function is the entry point for the downloader service process.
    It will import the service and run its test.
    """
    # This import happens in the child process
    from screenshot_system.download_service import DownloaderService

    service = DownloaderService()
    service.run()

if __name__ == "__main__":
    print("--- libtorrent Version Test ---")
    print(f"[Main] libtorrent version in main process: {MAIN_LT_VERSION}")

    # Clean up old log file
    if os.path.exists('downloader_service.log'):
        os.remove('downloader_service.log')

    print("[Main] Starting Downloader Service for version test...")
    downloader_process = multiprocessing.Process(
        target=downloader_service_bootstrap,
        daemon=True
    )
    downloader_process.start()

    # Wait for the child process to finish its work (logging to the file)
    print("[Main] Waiting for service process to complete test...")
    downloader_process.join(timeout=15)

    if downloader_process.is_alive():
        print("[Main] Test process timed out.")
        downloader_process.terminate()

    print("\n--- Test Complete ---")
    print("Please check the contents of the 'downloader_service.log' file for the service's version report.")
    print("Compare it with the version printed above for the main process.")
