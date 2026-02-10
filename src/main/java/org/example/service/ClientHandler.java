package org.example.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class ClientHandler implements Runnable {
    private final SocketChannel socket;

    private final TopicLog topicLog;

    public ClientHandler(SocketChannel socket, TopicLog topicLog) {
        this.socket = socket;
        this.topicLog = topicLog;
    }

    @Override
    public void run() {
        // to handle "PRODUCE" and "CONSUME" commands.
        try {
            // Keep connection open for now to simulate a session
            System.out.println("Received a request");

            socket.configureBlocking(true);
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            while(socket.read(buffer) != -1){
                buffer.flip();

                while(buffer.remaining()>4){

                    buffer.mark();
                    int sizeOfNextRequest = buffer.getInt();

                    if(buffer.remaining()<sizeOfNextRequest){
                        buffer.reset();
                        break;
                    }

                    int originalLimit = buffer.limit();
                    buffer.limit(buffer.position()+sizeOfNextRequest);

                    //getting the request buffer
                    ByteBuffer requestBuffer = buffer.slice();

                    //process the request now
                    processRequest(requestBuffer);

                    //moving the cursor past previous request
                    buffer.position(buffer.position()+sizeOfNextRequest);
                    buffer.limit(originalLimit);

                }
                buffer.compact();

            }

            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processRequest(ByteBuffer buffer) throws IOException {
        short commandLength = buffer.getShort();

        byte[] commandBytes = new byte[commandLength];
        buffer.get(commandBytes);

        String command = new String(commandBytes, StandardCharsets.UTF_8);

        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        short partition = buffer.getShort();

        int messageLength = buffer.getInt();
        byte[] messageBytes = new byte[messageLength];
        buffer.get(messageBytes);
        String message = new String(messageBytes, StandardCharsets.UTF_8);

        System.out.println("Received : " + command+" "+topic+" "+partition+" "+message);

    }

    private void parseCommand(String command, String topic, int partition, byte[] messageBytes) throws IOException {
        if(command.equals("CREATE-TOPIC")){
            topicLog.setupStorage(topic, partition);
        }
        if(command.equals("PRODUCE")){
            topicLog.append(messageBytes);

        }
        if(command.equals("CONSUME")){

        }

    }

    private void parseAndProcessCommands(ByteBuffer buffer){
        boolean lineFound = false;
        StringBuilder stringBuilder = new StringBuilder();
        while(buffer.hasRemaining()){
            char c = (char) buffer.get();
            if(c=='\n'){
                lineFound = true;
                break;
            }
            stringBuilder.append(c);
            System.out.println(stringBuilder.toString());
        }

        String command = stringBuilder.toString().trim();
        handleCommand(command);

    }

    private void handleCommand(String command){
        System.out.print("Handling command ");
        System.out.println(command);
    }


}
