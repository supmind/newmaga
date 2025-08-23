import io
import os
import time

class TorrentFileIO(io.RawIOBase):
    def __init__(self, infohash: str, file_index: int, file_size: int, request_queue, result_dict):
        self.infohash = infohash.upper()
        self.file_index = file_index
        self.file_size = file_size
        self.request_queue = request_queue
        self.result_dict = result_dict

        self.pos = 0
        self._piece_length = self._get_piece_length()
        if not self._piece_length:
            raise IOError(f"Failed to get torrent metadata for {self.infohash}")

    def _get_piece_length(self, timeout=300): # 5 minutes
        """
        Gets the piece length for the torrent by requesting metadata from the service.
        """
        self.request_queue.put(('get_metadata', self.infohash))

        start_time = time.time()
        while time.time() - start_time < timeout:
            metadata = self.result_dict.get(('metadata', self.infohash))
            if metadata:
                return metadata['piece_length']
            time.sleep(0.5)
        return None


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
        if self.pos >= self.file_size:
            return b''

        read_size = min(size if size != -1 else self.file_size, self.file_size - self.pos)
        if read_size <= 0:
            return b''

        start_offset = self.pos
        end_offset = start_offset + read_size

        start_piece, start_piece_offset = divmod(start_offset, self._piece_length)
        end_piece, _ = divmod(end_offset - 1, self._piece_length)

        pieces_needed = range(start_piece, end_piece + 1)

        for piece_index in pieces_needed:
            if (self.infohash, piece_index) not in self.result_dict:
                self.request_queue.put(('get_piece', (self.infohash, piece_index)))

        all_pieces_data = {}
        for piece_index in pieces_needed:
            data = self._wait_for_piece(piece_index, timeout=1800)
            if not data:
                return b''
            all_pieces_data[piece_index] = data

        full_chunk = b"".join(all_pieces_data[i] for i in sorted(all_pieces_data.keys()))

        final_start = start_piece_offset
        final_end = final_start + read_size
        result = full_chunk[final_start:final_end]

        self.pos += len(result)
        return result

    def _wait_for_piece(self, piece_index, timeout=60):
        start_time = time.time()
        key = ('piece', self.infohash, piece_index)

        # First check if it's already there
        data = self.result_dict.get(key)
        if data:
            return data

        # If not, wait for it
        while time.time() - start_time < timeout:
            data = self.result_dict.get(key)
            if data:
                return data
            time.sleep(0.2)
        return None

    def close(self):
        pass
