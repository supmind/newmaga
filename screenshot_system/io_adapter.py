import io
import uuid
from queue import Empty

class TorrentFileIO(io.RawIOBase):
    def __init__(self, infohash: str, file_index: int, file_size: int, task_queue, result_dispatcher):
        self.infohash = infohash
        self.file_index = file_index
        self.file_size = file_size

        self.task_queue = task_queue
        self.dispatcher = result_dispatcher

        self.pos = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        self.pos = offset
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
        response_queue = self.dispatcher.add_request(request_id)

        task = {
            'type': 'DOWNLOAD_RANGE',
            'request_id': request_id,
            'infohash': self.infohash,
            'file_index': self.file_index,
            'offset': self.pos,
            'size': read_size,
        }

        print(f"[IOAdapter] Sending request {request_id} for {read_size} bytes at offset {self.pos}")
        self.task_queue.put(task)

        data = b''
        try:
            # Block and wait for the result from the downloader service via the dispatcher
            result = response_queue.get(timeout=190) # 3 min timeout + buffer
            error = result.get('error')
            if error:
                print(f"[IOAdapter] Request {request_id} failed with error: {error}")
            else:
                data = result.get('data', b'')
                print(f"[IOAdapter] Request {request_id} received {len(data)} bytes.")
        except Empty:
            print(f"[IOAdapter] Request {request_id} timed out.")
        except Exception as e:
            print(f"[IOAdapter] Request {request_id} failed with exception: {e}")
        finally:
            self.dispatcher.remove_request(request_id)

        if data:
            self.pos += len(data)

        return data

    def close(self):
        pass
