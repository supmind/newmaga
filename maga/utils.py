import binascii
import os
from socket import inet_ntoa
from struct import unpack
import collections


def proper_infohash(infohash):
    if isinstance(infohash, bytes):
        # Convert bytes to hex
        infohash = binascii.hexlify(infohash).decode('utf-8')
    return infohash.upper()


def random_node_id(size=20):
    return os.urandom(size)


def split_nodes(nodes):
    length = len(nodes)
    if (length % 26) != 0:
        return

    for i in range(0, length, 26):
        nid = nodes[i:i+20]
        ip = inet_ntoa(nodes[i+20:i+24])
        port = unpack("!H", nodes[i+24:i+26])[0]
        yield nid, ip, port


def get_distance(node1_id, node2_id):
    """
    Calculate the XOR distance between two node IDs.
    """
    return int.from_bytes(node1_id, 'big') ^ int.from_bytes(node2_id, 'big')


def split_peers(peers):
    """
    Parses a compact peer list.
    """
    length = len(peers)
    if (length % 6) != 0:
        return

    for i in range(0, length, 6):
        ip = inet_ntoa(peers[i:i+4])
        port = unpack("!H", peers[i+4:i+6])[0]
        yield ip, port


class BoundedSet:
    """
    A set-like data structure with a fixed maximum size, implemented
    with an OrderedDict to provide O(1) for add, remove, and contains.
    When full, adding a new item discards the oldest item.
    """
    def __init__(self, max_size=1_000_000):
        self.max_size = max_size
        self.data = collections.OrderedDict()

    def add(self, item):
        if item in self.data:
            return False  # Item already exists

        if len(self.data) >= self.max_size:
            self.data.popitem(last=False)  # Remove the oldest item

        self.data[item] = None  # Add the new item
        return True

    def __contains__(self, item):
        return item in self.data

    def remove(self, item):
        """Removes an item from the set."""
        if item in self.data:
            del self.data[item]
            return True
        return False

    def __len__(self):
        return len(self.data)


def format_bytes(size):
    """Formats a size in bytes into a human-readable string (KB, MB, GB)."""
    if size is None:
        return "N/A"
    power = 1024
    n = 0
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size >= power and n < len(power_labels) - 1:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}"
