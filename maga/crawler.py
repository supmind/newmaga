import asyncio
import os
import signal
import socket
import time
import uvloop

uvloop.install()

from socket import inet_ntoa
from struct import unpack
import io

from datetime import datetime, timezone
import random
import collections
from fastbencode import bencode, bdecode
import logging

from . import utils
from . import constants


__version__ = '3.0.0'

K = 8


class Maga(asyncio.DatagramProtocol):
    def __init__(self, loop=None, bootstrap_nodes=constants.BOOTSTRAP_NODES, interval=1, handler=None,
                 node_queue_maxsize=500, node_processor_concurrency=10):
        self.node_id = utils.random_node_id()
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()
        if handler:
            self.handler = handler
        self.log = logging.getLogger("Crawler")
        self._pending_queries = {}
        self.background_tasks = set()
        self.rate_limiter = {}

        self.node_processor_concurrency = node_processor_concurrency
        self.node_queue = asyncio.Queue(maxsize=node_queue_maxsize)
        self.node_processor_tasks = []

        resolved_bootstrap_nodes = []
        for host, port in bootstrap_nodes:
            try:
                ip = socket.gethostbyname(host)
                addr = (ip, port)
                resolved_bootstrap_nodes.append(addr)
                # Bootstrap nodes don't have IDs, so we can't place them in k-buckets yet.
                # The _add_node logic will handle them when they communicate.
            except socket.gaierror:
                pass
        self.bootstrap_nodes = tuple(resolved_bootstrap_nodes)

        self.__running = False
        self.interval = interval
        self.find_nodes_task = None
        self.cleanup_task = None

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport.close()
        self.__running = False
        super().connection_lost(exc)

    def datagram_received(self, data, addr):
        now = time.monotonic()
        ip = addr[0]

        if ip not in self.rate_limiter:
            self.rate_limiter[ip] = collections.deque()

        timestamps = self.rate_limiter[ip]

        # Remove timestamps older than the window
        while timestamps and timestamps[0] < now - constants.RATE_LIMIT_WINDOW:
            timestamps.popleft()

        if len(timestamps) >= constants.RATE_LIMIT_REQUESTS:
            # Drop packet
            return

        timestamps.append(now)

        try:
            msg = bdecode(data)
        except:
            return
        try:
            self.handle_message(msg, addr)
        except Exception as e:
            self.send_message(data={
                constants.KRPC_T: msg.get(constants.KRPC_T),
                constants.KRPC_Y: constants.KRPC_ERROR,
                constants.KRPC_E: constants.KRPC_SERVER_ERROR
            }, addr=addr)
            raise e

    def send_message(self, data, addr):
        data.setdefault(constants.KRPC_T, constants.KRPC_DEFAULT_TID)
        self.transport.sendto(bencode(data), addr)

    def handle_message(self, msg, addr):
        msg_type = msg.get(constants.KRPC_Y, constants.KRPC_ERROR)
        if msg_type == constants.KRPC_ERROR:
            return

        node_id = None
        try:
            node_id = msg[constants.KRPC_A][constants.KRPC_ID]
            self._add_node_to_queue(node_id, addr)
        except KeyError:
            # This can happen on response messages.
            pass

        if msg_type == constants.KRPC_RESPONSE:
            tid = msg.get(constants.KRPC_T)
            r_args = msg.get(constants.KRPC_R, {})
            # The original `msg` object is now out of scope and can be collected.
            return self.handle_response(tid, r_args, addr)
        elif msg_type == constants.KRPC_QUERY:
            tid = msg.get(constants.KRPC_T)
            q_type = msg.get(constants.KRPC_Q)
            a_args = msg.get(constants.KRPC_A, {})
            # The original `msg` object is now out of scope and can be collected.
            task = asyncio.ensure_future(
                self.handle_query(tid, q_type, a_args, addr), loop=self.loop
            )
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
            return task

    def stop(self):
        self.__running = False
        if self.find_nodes_task:
            self.find_nodes_task.cancel()
        if self.cleanup_task:
            self.cleanup_task.cancel()
        for task in self.node_processor_tasks:
            task.cancel()
        if self.transport:
            self.transport.close()

    async def _node_processor(self):
        """
        Pulls nodes from the queue and processes them.
        """
        while self.__running:
            try:
                node_id, addr = await self.node_queue.get()
                self.find_node(addr=addr, node_id=node_id)
                self.node_queue.task_done()
            except asyncio.CancelledError:
                self.log.info("Node processor task cancelled.")
                break
            except Exception:
                self.log.exception("Error in node processor task.")

    async def auto_find_nodes(self):
        self.__running = True
        while self.__running:
            try:
                await asyncio.sleep(self.interval)
                for node in self.bootstrap_nodes:
                    self.find_node(addr=node)
            except Exception:
                self.log.exception("Error in Crawler auto_find_nodes loop")

    async def _cleanup_rate_limiter(self):
        """
        Periodically cleans up the rate_limiter dictionary to remove stale entries.
        """
        while self.__running:
            try:
                await asyncio.sleep(constants.RATE_LIMIT_CLEANUP_INTERVAL)

                now = time.monotonic()
                initial_size = len(self.rate_limiter)

                # Create a list of IPs to remove to avoid modifying the dict while iterating
                stale_ips = [
                    ip for ip, timestamps in self.rate_limiter.items()
                    if not timestamps or timestamps[-1] < now - constants.RATE_LIMIT_CLEANUP_INTERVAL
                ]

                for ip in stale_ips:
                    del self.rate_limiter[ip]

                final_size = len(self.rate_limiter)
                if initial_size > 0:
                    self.log.info(
                        f"Rate limiter cleanup: "
                        f"Removed {len(stale_ips)} stale entries. "
                        f"Size changed from {initial_size} to {final_size}."
                    )

            except asyncio.CancelledError:
                self.log.info("Rate limiter cleanup task cancelled.")
                break
            except Exception:
                self.log.exception("Error in rate limiter cleanup task.")

    async def run(self, port=6881):
        _, _ = await self.loop.create_datagram_endpoint(
                lambda: self, local_addr=('0.0.0.0', port)
        )

        for node in self.bootstrap_nodes:
            # Bootstrap
            self.find_node(addr=node, node_id=self.node_id)

        task = asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
        self.find_nodes_task = task

        cleanup_task = asyncio.ensure_future(self._cleanup_rate_limiter(), loop=self.loop)
        self.background_tasks.add(cleanup_task)
        cleanup_task.add_done_callback(self.background_tasks.discard)
        self.cleanup_task = cleanup_task

        for _ in range(self.node_processor_concurrency):
            task = asyncio.ensure_future(self._node_processor(), loop=self.loop)
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
            self.node_processor_tasks.append(task)

    def handle_response(self, tid, r_args, addr):
        # A response from a node we know about is a good sign.
        # This is already handled in handle_message, so we don't need to repeat it.

        if tid in self._pending_queries:
            # The future is just waiting for any valid response, not a specific one.
            # The caller will be responsible for parsing the response.
            future = self._pending_queries.pop(tid)
            # We pass the smaller `r_args` dict, not the whole `msg`.
            future.set_result(r_args)
            return

        # In addition to our own queries, we also learn about new nodes
        # from other nodes' responses to us.
        if constants.KRPC_NODES in r_args:
            for node_id, ip, port in utils.split_nodes(r_args[constants.KRPC_NODES]):
                self._add_node_to_queue(node_id, (ip, port))

    async def handle_query(self, tid, q_type, a_args, addr):
        node_id = a_args.get(constants.KRPC_ID)
        query_type = q_type

        if not all([node_id, query_type]):
            return

        if query_type == constants.KRPC_GET_PEERS:
            infohash = a_args[constants.KRPC_INFO_HASH]
            token = infohash[:2]
            self.send_message({
                constants.KRPC_T: tid,
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id),
                    constants.KRPC_NODES: b"",
                    constants.KRPC_TOKEN: token
                }
            }, addr=addr)
            await self.handle_get_peers(infohash, addr)
        elif query_type == constants.KRPC_ANNOUNCE_PEER:
            infohash = a_args[constants.KRPC_INFO_HASH]
            self.send_message({
                constants.KRPC_T: tid,
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id)
                }
            }, addr=addr)

            if a_args.get(constants.KRPC_IMPLIED_PORT, 0) != 0:
                peer_port = addr[1]
            else:
                peer_port = a_args[constants.KRPC_PORT]
            peer_addr = (addr[0], peer_port)

            await self.handle_announce_peer(infohash, addr, peer_addr)
        elif query_type == constants.KRPC_FIND_NODE:
            self.send_message({
                constants.KRPC_T: tid,
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id),
                    constants.KRPC_NODES: b""
                }
            }, addr=addr)
        elif query_type == constants.KRPC_PING:
            self.send_message({
                constants.KRPC_T: tid,
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id)
                }
            }, addr=addr)

        self.find_node(addr=addr, node_id=node_id)

    def ping(self, addr, node_id=None):
        self.send_message({
            constants.KRPC_Y: constants.KRPC_QUERY,
            constants.KRPC_T: constants.KRPC_PING_TID,
            constants.KRPC_Q: constants.KRPC_PING,
            constants.KRPC_A: {
                constants.KRPC_ID: self.fake_node_id(node_id)
            }
        }, addr=addr)

    def fake_node_id(self, node_id=None):
        if node_id:
            return node_id[:-1]+self.node_id[-1:]
        return self.node_id

    def find_node(self, addr, node_id=None, target=None):
        if not target:
            target = utils.random_node_id()
        self.send_message({
            constants.KRPC_T: constants.KRPC_FIND_NODE_TID,
            constants.KRPC_Y: constants.KRPC_QUERY,
            constants.KRPC_Q: constants.KRPC_FIND_NODE,
            constants.KRPC_A: {
                constants.KRPC_ID: self.fake_node_id(node_id),
                constants.KRPC_TARGET: target
            }
        }, addr=addr)

    async def _send_query_and_wait(self, query_data, addr, timeout=2):
        """
        Sends a query to a specific address and waits for a response.
        """
        tid = os.urandom(2)
        query_data[constants.KRPC_T] = tid

        future = self.loop.create_future()
        self._pending_queries[tid] = future

        try:
            self.send_message(query_data, addr)
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self._pending_queries.pop(tid, None)



    def _add_node_to_queue(self, node_id, addr):
        try:
            if self.node_queue.full():
                # Remove the oldest item to make space
                self.node_queue.get_nowait()
                # self.log.warning("Node processing queue is full, dropping oldest node.")
            self.node_queue.put_nowait((node_id, addr))
        except asyncio.QueueFull:
            # This should technically not be reached if we check full() first,
            # but as a safeguard in case of race conditions (though unlikely here).
            self.log.error("Failed to add node to queue even after attempting to make space.")


    async def handler(self, infohash, addr, peer_addr=None):
        """
        Default handler for discovered infohashes. Does nothing.
        """
        raise NotImplementedError

    async def handle_get_peers(self, infohash, addr):
        try:
            await self.handler(infohash, addr)
        except NotImplementedError:
            pass

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        try:
            await self.handler(infohash, addr, peer_addr=peer_addr)
        except NotImplementedError:
            pass
