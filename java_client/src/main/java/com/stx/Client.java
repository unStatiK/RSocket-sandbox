package com.stx;

import com.google.protobuf.InvalidProtocolBufferException;
import com.stx.model.Msg;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.nio.ByteBuffer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import reactor.core.publisher.Mono;

public class Client {

    private static final String HOST_OPTION_NAME = "host";
    private static final String PORT_OPTION_NAME = "port";
    private static final String VERSION_OPTION_NAME = "version";
    private static final String TYPE_OPTION_NAME = "type";

    public static void main(String[] args) throws InvalidProtocolBufferException {
        CommandLine cmd = parseArgs(args);
        if (cmd == null) {
            System.exit(1);
        }

        String serverHost = cmd.getOptionValue(HOST_OPTION_NAME);
        int serverPort = Integer.parseInt(cmd.getOptionValue(PORT_OPTION_NAME));
        int protocolVersion = Integer.parseInt(cmd.getOptionValue(VERSION_OPTION_NAME));
        int type = Integer.parseInt(cmd.getOptionValue(TYPE_OPTION_NAME));

        RSocket clientRSocket = createClient(serverHost, serverPort);
        try {
            switch (protocolVersion) {
                case 1:
                    processV1Client(clientRSocket, type);
                    break;
                case 2:
                    processV2Client(clientRSocket, type);
                    break;
                case 3:
                    processV3Client(clientRSocket, type);
                    break;
            }
        } finally {
            clientRSocket.dispose();
        }
    }

    private static void processV1Client(RSocket clientRSocket, int type) throws InvalidProtocolBufferException {
        Msg.Wrapper wrapper = Msg.Wrapper.newBuilder()
            .setType(type)
            .build();
        Mono<Payload> response = clientRSocket.requestResponse(ByteBufPayload.create(wrapper.toByteArray()));
        Payload responseData = response.single().block();
        Msg.Wrapper responseEntity = Msg.Wrapper.parseFrom(responseData.getData());
        int responseType = responseEntity.getType();
        System.out.println("Get response type: " + responseType);
        processMsg(responseType, responseEntity.getData().asReadOnlyByteBuffer());
    }

    private static void processV2Client(RSocket clientRSocket, int type) throws InvalidProtocolBufferException {
        final byte[] typeBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(type).array();
        Mono<Payload> response = clientRSocket.requestResponse(ByteBufPayload.create(new byte[]{}, typeBuffer));
        Payload responseData = response.single().block();
        int responseType = responseData.getMetadata().getInt();
        System.out.println("Get response type: " + responseType);
        processMsg(responseType, responseData.getData());
    }

    private static void processV3Client(RSocket clientRSocket, int type) throws InvalidProtocolBufferException {
        final byte[] typeBuffer = ByteBuffer.allocate(Byte.BYTES).put(convertTypeToOpcode(type)).array();
        Mono<Payload> response = clientRSocket.requestResponse(ByteBufPayload.create(new byte[]{}, typeBuffer));
        Payload responseData = response.single().block();
        int responseType = convertOpcodeToType(responseData.getMetadata().get());
        System.out.println("Get response type: " + responseType);
        processMsg(responseType, responseData.getData());
    }

    private static void processMsg(int type, ByteBuffer data) throws InvalidProtocolBufferException {
        switch (type) {
            case 1:
                processMsgStatus(data);
                break;
            case 2:
                processMsgContainer(data);
                break;
        }
    }

    private static void processMsgStatus(ByteBuffer data) throws InvalidProtocolBufferException {
        Msg.MsgStatus statusEntity = Msg.MsgStatus.parseFrom(data);
        System.out.println("Get response status: " + statusEntity.getStatus());
    }

    private static void processMsgContainer(ByteBuffer data) throws InvalidProtocolBufferException {
        Msg.MsgContainer containerEntity = Msg.MsgContainer.parseFrom(data);
        System.out.println("Get response tag: " + containerEntity.getTag());
        System.out.println("Response contains " + containerEntity.getPacketsCount() + " packets");
        for (int i = 0; i < containerEntity.getPacketsCount(); i++) {
            System.out.println("######################################################");
            System.out.println("packet id: " + containerEntity.getPackets(i).getId());
            System.out.println("packet name: " + containerEntity.getPackets(i).getName());
        }
    }

    private static byte convertTypeToOpcode(int type) {
        if (type == 1) {
            return (byte)0x07;
        }
        if (type == 2) {
            return (byte)0x09;
        }
        return (byte)0x00;
    }

    private static int convertOpcodeToType(byte opcode) {
        if (opcode == 0x07) {
            return 1;
        }
        if (opcode == 0x09) {
            return 2;
        }
        return 0;
    }

    private static CommandLine parseArgs(String[] args) {
        Options options = new Options();
        Option host = new Option("h", HOST_OPTION_NAME, true, "server host");
        host.setRequired(true);
        options.addOption(host);

        Option port = new Option("p", PORT_OPTION_NAME, true, "server port");
        port.setRequired(true);
        options.addOption(port);

        Option version = new Option("v", VERSION_OPTION_NAME, true, "protocol version");
        version.setRequired(true);
        options.addOption(version);

        Option type = new Option("t", TYPE_OPTION_NAME, true, "msg type [1 - status, 2 - container]");
        type.setRequired(true);
        options.addOption(type);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Client", options);
            return null;
        }
    }

    private static RSocket createClient(String address, int port) {
        return RSocketConnector.create()
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .connect(TcpClientTransport.create(address, port))
            .block();
    }
}
