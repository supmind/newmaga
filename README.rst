Maga: A High-Performance DHT Crawler Framework
==============================================

Maga is a powerful and easy-to-use framework for building DHT crawlers, written in Python with ``asyncio`` and accelerated by ``uvloop``. It allows you to listen to the BitTorrent DHT network, discover infohashes, and fetch metadata for torrents.

Features
--------

*   **High-Performance**: Built on ``asyncio`` and ``uvloop`` for excellent performance and scalability.
*   **Simple API**: Get a crawler running in just a few lines of code.
*   **Extensible**: Provides simple handlers (``handle_get_peers``, ``handle_announce_peer``) that you can override to process incoming data.
*   **Metadata Downloader**: Includes a utility to easily download torrent metadata from discovered peers.
*   **Utility Tools**: Comes with ready-to-use tools for diagnostics and testing.

Installation
------------

1.  Clone the repository:

    .. code-block:: bash

        git clone https://github.com/whtsky/maga.git
        cd maga

2.  Install the required dependencies:

    .. code-block:: bash

        pip install -r requirements.txt

3.  Install the project in editable mode to ensure the modules are available in your path:

    .. code-block:: bash

        pip install -e .

Quick Start
-----------

The ``main.py`` file provides a complete example of a robust metadata crawler. It uses a producer-consumer model where the crawler discovers infohashes and a pool of workers downloads the metadata concurrently.

To run it:

.. code-block:: bash

    python main.py --port 6881 --workers 200

You can customize the DHT listening port and the number of download workers.

Here is a simplified version of the code from ``main.py``:

.. code-block:: python

    import asyncio
    import logging
    from maga.crawler import Maga
    from maga.downloader import get_metadata
    from maga.utils import proper_infohash

    logging.basicConfig(level=logging.INFO)

    class MyCrawler(Maga):
        async def handle_announce_peer(self, infohash, addr, peer_addr):
            infohash_hex = proper_infohash(infohash)
            print(f"Discovered {infohash_hex} from {peer_addr}")

            # You can now download the metadata
            # info = await get_metadata(infohash, peer_addr[0], peer_addr[1])
            # if info:
            #     print(f"Successfully downloaded metadata for {infohash_hex}")

    async def main():
        crawler = MyCrawler()
        await crawler.run()
        # The crawler will run forever until stopped
        await asyncio.Event().wait()

    if __name__ == "__main__":
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            pass


Advanced Tools
--------------

The project includes additional scripts for diagnostics and testing.

### `getinfohash.py`: Diagnostics Tool

This script runs the crawler without processing infohashes and prints detailed statistics about the DHT routing table and node queue every 30 seconds. It's useful for monitoring the health and status of your crawler.

**Usage:**

.. code-block:: bash

    python getinfohash.py

### `getpeer.py`: Get Peers Tool

This tool allows you to test the ``get_peers`` functionality for a specific infohash. It warms up a routing table and then performs a multi-hop query to find peers for the given infohash.

**Usage:**

.. code-block:: bash

    # Replace <infohash> with the torrent infohash you want to look up
    python getpeer.py <infohash>

API Overview
------------

To build your own crawler, you simply subclass ``maga.Maga`` and override one or both of its handlers:

*   ``async def handle_get_peers(self, infohash, addr)``: This handler is called when a ``get_peers`` query is received from another node. ``addr`` is the address of the querying node.
*   ``async def handle_announce_peer(self, infohash, addr, peer_addr)``: This handler is called when an ``announce_peer`` query is received. ``addr`` is the address of the announcing DHT node, and ``peer_addr`` is the address of the peer that is part of the torrent swarm.
