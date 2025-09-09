import asyncio
import logging
import random
import signal
import argparse
import binascii

from maga.crawler import Maga
from maga import utils
from maga import constants

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestCrawler(Maga):
    """A simple crawler that does nothing, just for populating the routing table."""
    async def handle_get_peers(self, infohash, addr):
        pass

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        pass

async def perform_two_hop_get_peers(crawler: Maga, infohash: bytes, starting_nodes: list):
    """
    Manually performs a two-hop get_peers query starting from a given set of nodes.
    """
    logging.info(f"Starting two-hop get_peers for infohash: {binascii.hexlify(infohash).decode()}")
    logging.info(f"Starting with {len(starting_nodes)} random nodes.")

    found_peers = set()
    queried_nodes = set()
    
    nodes_to_query = starting_nodes
    
    for hop in range(1, 3): # Two hops
        if not nodes_to_query:
            logging.info(f"Hop {hop}: No new nodes to query. Stopping.")
            break

        logging.info(f"Hop {hop}: Querying {len(nodes_to_query)} nodes...")
        
        query_data = {
            constants.KRPC_Y: constants.KRPC_QUERY,
            constants.KRPC_Q: constants.KRPC_GET_PEERS,
            constants.KRPC_A: {
                constants.KRPC_ID: crawler.node_id,
                constants.KRPC_INFO_HASH: infohash
            }
        }

        tasks = [crawler._send_query_and_wait(query_data, addr) for addr in nodes_to_query]
        queried_nodes.update(nodes_to_query)

        responses = await asyncio.gather(*tasks)

        new_nodes_found = []
        for r_args in responses:
            if not r_args:
                continue

            if constants.KRPC_VALUES in r_args:
                for peer in utils.split_peers(r_args[constants.KRPC_VALUES]):
                    found_peers.add(peer)

            if constants.KRPC_NODES in r_args:
                for _, ip, port in utils.split_nodes(r_args[constants.KRPC_NODES]):
                    addr = (ip, port)
                    if addr not in queried_nodes:
                        new_nodes_found.append(addr)
        
        logging.info(f"Hop {hop}: Found {len(found_peers)} peers so far. Discovered {len(new_nodes_found)} new nodes for next hop.")

        if found_peers:
            logging.info("Peers found, stopping search as per standard DHT behavior.")
            break

        nodes_to_query = new_nodes_found
        
    return found_peers

async def main(args):
    loop = asyncio.get_running_loop()

    try:
        infohash = binascii.unhexlify(args)
    except (ValueError, TypeError):
        logging.error(f"Invalid infohash provided: {args}")
        return

    crawler = TestCrawler()
    await crawler.run(port=0)
    logging.info("Crawler started. Warming up routing table for 60 seconds...")

    try:
        await asyncio.sleep(60)
    except asyncio.CancelledError:
        logging.info("Warm-up interrupted.")
        crawler.stop()
        return

    logging.info("Warm-up complete. Starting get_peers test...")

    # --- Node Selection ---
    all_nodes = [node for bucket in crawler.k_buckets for node in bucket]
    if not all_nodes:
        logging.error("Routing table is empty after warm-up. Cannot perform test.")
        crawler.stop()
        return
        
    num_to_select = min(20, len(all_nodes))
    starting_nodes_full = random.sample(all_nodes, num_to_select)
    starting_node_addrs = [node['addr'] for node in starting_nodes_full]

    # --- Perform Test ---
    found_peers = await perform_two_hop_get_peers(crawler, infohash, starting_node_addrs)

    logging.info("="*20 + " Test Complete " + "="*20)
    logging.info(f"Query for infohash {args.infohash} finished.")
    logging.info(f"Total unique peers found: {len(found_peers)}")
    if found_peers:
        logging.info("Some of the peers found:")
        for peer in list(found_peers)[:5]: # Print first 5
             logging.info(f"  - {peer[0]}:{peer[1]}")
    logging.info("="*55)
    
    # --- Shutdown ---
    logging.info("Stopping crawler...")
    crawler.stop()

if __name__ == "__main__":
    infohash = "177E05AC445B2F67A0898A14AA9238CF754FC22C"

    try:
        asyncio.run(main(infohash))
    except KeyboardInterrupt:
        logging.info("\nScript interrupted by user.")
