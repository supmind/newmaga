import io
import time
import bencode2 as bencoder

class TorrentFileIO(io.RawIOBase):
    def __init__(self, infohash: str, metadata: dict, file_index: int, file_size: int, request_queue, result_dict):
        self.infohash = infohash.upper()
        self.metadata = metadata
        self.file_index = file_index
        self.file_size = file_size
        self.request_queue = request_queue
        self.result_dict = result_dict

        self.pos = 0

        # Get piece length directly from the metadata
        try:
            # The metadata dict is the 'info' dict from the torrent.
            self._piece_length = self.metadata[b'piece length']
        except KeyError:
            raise IOError(f"Invalid metadata for {self.infohash}: 'piece length' not found.")

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

        # Request all pieces we don't have from the service
        for piece_index in pieces_needed:
            key = ('piece', self.infohash, piece_index)
            if key not in self.result_dict:
                self.request_queue.put(('get_piece', (self.infohash, piece_index)))

        # Wait for all pieces to be downloaded
        all_pieces_data = {}
        for piece_index in pieces_needed:
            data = self._wait_for_piece(piece_index, timeout=1800) # 30 min timeout per piece
            if not data:
                print(f"[{self.infohash}][IO] FATAL: Timeout or error waiting for piece {piece_index}")
                return b'' # Failed to get a piece
            all_pieces_data[piece_index] = data

        # Stitch pieces together
        full_chunk = b"".join(all_pieces_data[i] for i in sorted(all_pieces_data.keys()))

        # Slice the final result from the stitched chunk
        final_start = start_piece_offset
        final_end = final_start + read_size

        # Check if the stitched chunk is long enough
        if len(full_chunk) < final_end:
             print(f"[{self.infohash}][IO] FATAL: Stitched chunk is too small. "
                   f"Needed {final_end} bytes, got {len(full_chunk)}.")
             return b''

        result = full_chunk[final_start:final_end]

        self.pos += len(result)
        return result

    def _wait_for_piece(self, piece_index, timeout=60):
        start_time = time.time()
        key = ('piece', self.infohash, piece_index)

        while time.time() - start_time < timeout:
            data = self.result_dict.get(key)
            if data is not None:
                # Remove the piece from the dict to save memory
                # This assumes a piece is only ever needed once per read() call
                # which is a reasonable assumption.
                del self.result_dict[key]
                return data
            time.sleep(0.2)
        return None

    def close(self):
        pass
