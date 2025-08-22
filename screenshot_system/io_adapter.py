import io

class TorrentFileIO(io.RawIOBase):
    def __init__(self, downloader, infohash: str, file_index: int, file_size: int):
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
        if size == -1:
            size = self.file_size - self.pos

        if self.pos >= self.file_size:
            return b''

        read_size = min(size, self.file_size - self.pos)
        if read_size <= 0:
            return b''

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
        # The downloader session is managed by the worker process,
        # so we don't close it here.
        pass
