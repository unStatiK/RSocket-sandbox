
**Repository contains both python server and python and java clients.**

For start server :

> python3 server.py 1 // for start server with 1 version protocol

For clients :

> java -jar client.jar -h 127.0.0.1 -p 6565 -t 1 -v 1 // for send and get response with message type 1 and version protocol 1

> python3 client.py 127.0.0.1 6565 1 1 // for send and get response with message type 1 and version protocol 1


Protocol description:

- 1 version
Use only protobuf data, container wrapper contains message type and message data

- 2 version
Use pure protobuf message data without wrapper, message type is contained in the headers as integer


- 2 version
Use pure protobuf message data without wrapper, message type is contained in the headers as byte opcode
