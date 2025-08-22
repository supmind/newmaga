import io
from .downloader import Downloader
import libtorrent as lt

class TorrentFileIO(io.RawIOBase):
    """
    A file-like object that reads data from a torrent on demand.
    This class acts as an adapter between PyAV and our custom Downloader.
    """
    def __init__(self, downloader: Downloader, infohash: str, file_index: int, file_size: int):
        self.downloader = downloader
        self.infohash = infohash
        self.file_index = file_index
        self.file_size = file_size

        self.pos = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.pos = offset
        elif whence == io.SEEK_CUR:
            self.pos += offset
        elif whence == io.SEEK_END:
            self.pos = self.file_size + offset
        return self.pos

    def tell(self):
        return self.pos

    def read(self, size=-1):
        """
        Reads data from the torrent. This is where the magic happens.
        PyAV calls this method when it needs data.
        """
        if size == -1:
            size = self.file_size - self.pos

        if self.pos >= self.file_size:
            return b'' # End of file

        # Ensure we don't read past the end of the file
        read_size = min(size, self.file_size - self.pos)

        print(f"TorrentFileIO: Reading {read_size} bytes at offset {self.pos}")

        data = self.downloader.download_byte_range(
            self.infohash,
            self.file_index,
            self.pos,
            read_size
        )

        if data:
            self.pos += len(data)

        return data

    def close(self):
        # This can be a no-op as the downloader session is managed externally
        pass
