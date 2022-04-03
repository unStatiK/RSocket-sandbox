import sys
from msg_types import *

def get_reverse_order():
    byteorder = sys.byteorder
    if byteorder == 'little':
        return 'big'
    if byteorder == 'big':
        return 'little'

def convert_type_to_opcode(type):
    if type is not None:
        if type == MsgType.status:
               return MSG_STATUS_OPCODE
        if type == MsgType.container:
               return MSG_CONTAINER_OPCODE
    return None

def convert_opcode_to_type(type_opcode):
    if type_opcode is not None:
        if type_opcode == MSG_STATUS_OPCODE:
               return MsgType.status
        if type_opcode == MSG_CONTAINER_OPCODE:
               return MsgType.container
    return None
