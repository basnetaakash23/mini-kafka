package org.example.service;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ClientHandler implements Runnable {
    private final SocketChannel socket;

    public ClientHandler(SocketChannel socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        // TODO: In the next step, we will implement the input stream reading
        // to handle "PRODUCE" and "CONSUME" commands.
        try {
            // Keep connection open for now to simulate a session
            System.out.println("Received a request");

            socket.configureBlocking(true);
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            while(socket.read(buffer) != -1){
                buffer.flip();
                System.out.println("Flipped");
                parseAndProcessCommands(buffer);
                buffer.compact();

            }
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
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
