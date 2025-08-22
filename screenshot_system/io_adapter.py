import io
import uuid
from queue import Empty

class TorrentFileIO(io.RawIOBase):
    def __init__(self, infohash: str, file_index: int, file_size: int, task_queue, pending_requests_map, manager):
        self.infohash = infohash
        self.file_index = file_index
        self.file_size = file_size

        self.task_queue = task_queue
        self.pending_requests = pending_requests_map
        self.manager = manager

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

        request_id = uuid.uuid4().hex
        response_queue = self.manager.Queue(1)
        self.pending_requests[request_id] = response_queue

        task = {
            'type': 'DOWNLOAD_RANGE',
            'request_id': request_id,
            'infohash': self.infohash,
            'file_index': self.file_index,
            'offset': self.pos,
            'size': read_size,
        }

        self.task_queue.put(task)

        try:
            # Block and wait for the result from the downloader service via the dispatcher
            result = response_queue.get(timeout=180)
            data = result.get('data', b'')
        except Empty:
            data = b''
        except Exception:
            data = b''
        finally:
            # Clean up the map
            if request_id in self.pending_requests:
                del self.pending_requests[request_id]

        if data:
            self.pos += len(data)

        return data

    def close(self):
        pass
