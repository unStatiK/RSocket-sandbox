import asyncio
import logging
import argparse
import sys
import ctypes

from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP

import msg_pb2
import utils

def process_msg(type, data):
    if type == 1:
       process_status_msg(data)
    if type == 2:
       process_container_msg(data)

def process_status_msg(data):
    msg_status = msg_pb2.MsgStatus()
    msg_status.ParseFromString(data)
    logging.info('status message: {}'.format(msg_status.status))

def process_container_msg(data):
    msg_container = msg_pb2.MsgContainer()
    msg_container.ParseFromString(data)
    logging.info('tag message: {}'.format(msg_container.tag))
    logging.info('msg contains {} packets'.format(len(msg_container.packets)))
    for packet in msg_container.packets:
        logging.info('================')
        logging.info('packet id: {}'.format(packet.id))
        logging.info('packet name: {}'.format(packet.name))
        logging.info('================')

async def main(host, port, type, version):
    connection = await asyncio.open_connection(host, port)

    async with RSocketClient(TransportTCP(*connection)) as client:

        async def run_request_response_V1(type):
            try:
                msg = msg_pb2.Wrapper()
                msg.type = type
                data = msg.SerializeToString()
                payload = Payload(data)
                result = await client.request_response(payload)
                rmsg = msg_pb2.Wrapper()
                rmsg.ParseFromString(result.data)
                msg_type = rmsg.type
                logging.info('receive response msg with type: {}'.format(msg_type))
                process_msg(msg_type, rmsg.data)
            except asyncio.CancelledError:
                pass

        async def run_request_response_V2(type):
            try:
                meta_type_data = type.to_bytes(ctypes.sizeof(ctypes.c_int(type)), byteorder=utils.get_reverse_order())
                payload = Payload(b'', meta_type_data)
                result = await client.request_response(payload)
                msg_type = int.from_bytes(result.metadata, byteorder=utils.get_reverse_order(), signed=False)
                logging.info('receive response msg with type: {}'.format(msg_type))
                process_msg(msg_type, result.data)
            except asyncio.CancelledError:
                pass

        async def run_request_response_V3(type):
            try:
                payload = Payload(b'', utils.convert_type_to_opcode(type))
                result = await client.request_response(payload)
                opcode = bytes(result.metadata)
                opcode_string = "".join("0x{}".format(opcode.hex()))
                msg_type = utils.convert_opcode_to_type(opcode)
                logging.info('receive response msg with type: {}({})'.format(opcode_string, msg_type))
                process_msg(msg_type, result.data)
            except asyncio.CancelledError:
                pass


        if version == 1:
            await asyncio.create_task(run_request_response_V1(type))
        if version == 2:
            await asyncio.create_task(run_request_response_V2(type))
        if version == 3:
            await asyncio.create_task(run_request_response_V3(type))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('host', help='server host')
    parser.add_argument('port', help='server port')
    parser.add_argument('type', help='message type')
    parser.add_argument('version', help='protocol version')
    args = parser.parse_args()
    host = args.host
    port = int(args.port)
    type = int(args.type)
    version = int(args.version)
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main(host, port, type, version))
