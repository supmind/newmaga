import io
import os

# Helper for consistent logging
def log(infohash, message):
    print(f"[{infohash}][IO:{os.getpid()}] {message}")

class TorrentFileIO(io.RawIOBase):
    def __init__(self, downloader, infohash: str, file_index: int, file_size: int):
        self.downloader = downloader
        self.infohash = infohash
        self.file_index = file_index
        self.file_size = file_size
        self.pos = 0
        log(self.infohash, f"Initialized for file index {self.file_index} with size {self.file_size}")

    def readable(self):
        return True

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        log(self.infohash, f"seek({offset}, {whence}) called. Current pos: {self.pos}")
        if whence == io.SEEK_SET:
            self.pos = offset
        elif whence == io.SEEK_CUR:
            self.pos += offset
        elif whence == io.SEEK_END:
            self.pos = self.file_size + offset
        log(self.infohash, f"New pos: {self.pos}")
        return self.pos

    def tell(self):
        return self.pos

    def read(self, size=-1):
        log(self.infohash, f"read({size}) called. Current pos: {self.pos}")
        if size == -1:
            size = self.file_size - self.pos
            log(self.infohash, f"Read size defaulted to {size}")

        if self.pos >= self.file_size:
            log(self.infohash, "Read at EOF, returning b''")
            return b''

        read_size = min(size, self.file_size - self.pos)
        if read_size <= 0:
            log(self.infohash, f"Calculated read size is {read_size}, returning b''")
            return b''

        log(self.infohash, f"Requesting {read_size} bytes from downloader at offset {self.pos}")
        data = self.downloader.download_byte_range(
            self.infohash,
            self.file_index,
            self.pos,
            read_size
        )

        if data:
            self.pos += len(data)
            log(self.infohash, f"Downloader returned {len(data)} bytes. New pos: {self.pos}")
        else:
            log(self.infohash, "Downloader returned 0 bytes.")

        return data

    def close(self):
        log(self.infohash, "close() called.")
        # The downloader session is managed by the worker process,
        # so we don't close it here.
        pass
