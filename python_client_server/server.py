import asyncio
import logging
import ctypes
import sys
import argparse
import ujson

from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP

import msg_pb2
import utils
from msg_types import *

host = '0.0.0.0'
port = 6565

def extract_type_opcode(metadata):
    data = bytes(metadata)
    if data == b'':
        return None
    return data

def extract_msg_type_int_from_metadata(metadata):
    return int.from_bytes(metadata, byteorder=utils.get_reverse_order(), signed=False)

def extract_msg_type_opcode_from_metadata(metadata):
    return extract_type_opcode(metadata)

def make_wrapper_with_msg_status():
     msg = msg_pb2.Wrapper()
     msg.type = MsgType.status
     msg.data = make_msg_status()
     return msg.SerializeToString()

def make_msg_status():
     msg_status = msg_pb2.MsgStatus()
     msg_status.status = ujson.dumps({"OK": "200"})
     return msg_status.SerializeToString()

def make_wrapper_with_msg_container():
     msg = msg_pb2.Wrapper()
     msg.type = MsgType.container
     msg.data = make_msg_container()
     return msg.SerializeToString()

def make_msg_container():
     msg_container = msg_pb2.MsgContainer()
     msg_container.tag = 'r-tag'
     packet = msg_container.packets.add()
     packet.id = 999
     packet.name = 'p999'
     packet = msg_container.packets.add()
     packet.id = 1000
     packet.name = 'p1000'
     return msg_container.SerializeToString()

def make_response_v1(msg_type):
    data = None
    if msg_type is not None:
        response_future = asyncio.Future()
        if msg_type == MsgType.status:
               data = make_wrapper_with_msg_status()
        if msg_type == MsgType.container:
               data = make_wrapper_with_msg_container()
        response_future.set_result(Payload(data))
        return response_future

def make_response_v2(msg_type):
    data = None
    if msg_type is not None:
        response_future = asyncio.Future()
        if msg_type == MsgType.status:
               data = make_msg_status()
        if msg_type == MsgType.container:
               data = make_msg_container()
        meta_type_value = msg_type
        meta_type_data = meta_type_value.to_bytes(ctypes.sizeof(ctypes.c_int(meta_type_value)), byteorder=utils.get_reverse_order())
        response_future.set_result(Payload(data, meta_type_data))
        return response_future

def make_response_v3(msg_type):
    data = None
    if msg_type is not None:
        response_future = asyncio.Future()
        if msg_type == MSG_STATUS_OPCODE:
               data = make_msg_status()
        if msg_type == MSG_CONTAINER_OPCODE:
               data = make_msg_container()
        meta_type_value = msg_type
        response_future.set_result(Payload(data, meta_type_value))
        return response_future

class HandlerV1(BaseRequestHandler):
    async def request_response(self, payload: Payload) -> asyncio.Future:
        msg_wrapper = msg_pb2.Wrapper()
        msg_wrapper.ParseFromString(payload.data)
        msg_type = msg_wrapper.type
        logging.info("receive request with version 1 and msg with type {}\n".format(msg_type))
        return make_response_v1(msg_type)

class HandlerV2(BaseRequestHandler):
    async def request_response(self, payload: Payload) -> asyncio.Future:
        msg_type = extract_msg_type_int_from_metadata(payload.metadata)
        logging.info("receive request with version 2 and msg with type {}\n".format(msg_type))
        return make_response_v2(msg_type)

class HandlerV3(BaseRequestHandler):
    async def request_response(self, payload: Payload) -> asyncio.Future:
        msg_type = extract_msg_type_opcode_from_metadata(payload.metadata)
        opcode_string = "".join("0x{}".format(msg_type.hex()))
        logging.info("receive request with version 3 and msg with type {}({})\n".format(opcode_string, utils.convert_opcode_to_type(msg_type)))
        return make_response_v3(msg_type)

async def run_server(host, port, handler):
    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=handler)

    server = await asyncio.start_server(session, host, port)

    async with server:
        await server.serve_forever()


def resolve_handler(version):
    if version == 1:
        return HandlerV1
    if version == 2:
        return HandlerV2
    if version == 3:
        return HandlerV3

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Serve with protocol version.')
    parser.add_argument('version', help='protocol version')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    version = int(args.version)
    logging.info("serve with protocol version {} on address {}:{}\n".format(version, host, port))
    asyncio.run(run_server(host, port, resolve_handler(version)))
