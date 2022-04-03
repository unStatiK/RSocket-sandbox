import enum

class MsgType(enum.IntEnum):
    status = 1
    container = 2

MSG_STATUS_OPCODE = b'\x07'
MSG_CONTAINER_OPCODE = b'\x09'
