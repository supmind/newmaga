import asyncio
import os
import signal

from socket import inet_ntoa
from struct import unpack

import bencode2 as bencoder

from . import utils
from . import constants


__version__ = '3.0.0'


class KRPCProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop=None):
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport.close()

    def datagram_received(self, data, addr):
        try:
            msg = bencoder.bdecode(data)
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
        self.transport.sendto(bencoder.bencode(data), addr)

    def handle_message(self, msg, addr):
        msg_type = msg.get(constants.KRPC_Y, constants.KRPC_ERROR)

        if msg_type == constants.KRPC_ERROR:
            return

        if msg_type == constants.KRPC_RESPONSE:
            return self.handle_response(msg, addr=addr)

        if msg_type == constants.KRPC_QUERY:
            return asyncio.ensure_future(
                self.handle_query(msg, addr=addr), loop=self.loop
            )

    def handle_response(self, msg, addr):
        pass

    async def handle_query(self, msg, addr):
        pass


class Maga(KRPCProtocol):
    def __init__(self, loop=None, bootstrap_nodes=constants.BOOTSTRAP_NODES, interval=1):
        super().__init__(loop)
        self.node_id = utils.random_node_id()
        self.bootstrap_nodes = bootstrap_nodes
        self.__running = False
        self.interval = interval

    def stop(self):
        self.__running = False
        self.loop.call_later(self.interval, self.loop.stop)

    async def auto_find_nodes(self):
        self.__running = True
        while self.__running:
            await asyncio.sleep(self.interval)
            for node in self.bootstrap_nodes:
                self.find_node(addr=node)

    def run(self, port=6881):
        coro = self.loop.create_datagram_endpoint(
                lambda: self, local_addr=('0.0.0.0', port)
        )
        transport, _ = self.loop.run_until_complete(coro)

        for signame in ('SIGINT', 'SIGTERM'):
            try:
                self.loop.add_signal_handler(getattr(signal, signame), self.stop)
            except NotImplementedError:
                # SIGINT and SIGTERM are not implemented on windows
                pass

        for node in self.bootstrap_nodes:
            # Bootstrap
            self.find_node(addr=node, node_id=self.node_id)

        asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
        self.loop.run_forever()
        self.loop.close()

    def handle_response(self, msg, addr):
        args = msg[constants.KRPC_R]
        if constants.KRPC_NODES in args:
            for node_id, ip, port in utils.split_nodes(args[constants.KRPC_NODES]):
                self.ping(addr=(ip, port))

    async def handle_query(self, msg, addr):
        args = msg[constants.KRPC_A]
        node_id = args[constants.KRPC_ID]
        query_type = msg[constants.KRPC_Q]
        if query_type == constants.KRPC_GET_PEERS:
            infohash = args[constants.KRPC_INFO_HASH]
            infohash = utils.proper_infohash(infohash)
            token = infohash[:2]
            self.send_message({
                constants.KRPC_T: msg[constants.KRPC_T],
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id),
                    constants.KRPC_NODES: "",
                    constants.KRPC_TOKEN: token
                }
            }, addr=addr)
            await self.handle_get_peers(infohash, addr)
        elif query_type == constants.KRPC_ANNOUNCE_PEER:
            infohash = args[constants.KRPC_INFO_HASH]
            tid = msg[constants.KRPC_T]
            self.send_message({
                constants.KRPC_T: tid,
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id)
                }
            }, addr=addr)
            peer_addr = [addr[0], addr[1]]
            try:
                peer_addr[1] = args[constants.KRPC_PORT]
            except KeyError:
                pass
            await self.handle_announce_peer(utils.proper_infohash(infohash), addr, peer_addr)
        elif query_type == constants.KRPC_FIND_NODE:
            tid = msg[constants.KRPC_T]
            self.send_message({
                constants.KRPC_T: tid,
                constants.KRPC_Y: constants.KRPC_RESPONSE,
                constants.KRPC_R: {
                    constants.KRPC_ID: self.fake_node_id(node_id),
                    constants.KRPC_NODES: ""
                }
            }, addr=addr)
        elif query_type == constants.KRPC_PING:
            self.send_message({
                constants.KRPC_T: constants.KRPC_DEFAULT_TID,
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

    async def handle_get_peers(self, infohash, addr):
        await self.handler(infohash, addr)

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        await self.handler(infohash, addr)

    async def handler(self, infohash, addr):
        pass