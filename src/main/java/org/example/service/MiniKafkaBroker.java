package org.example.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MiniKafkaBroker {

    private final String topic;

    private final int partitions;

    private final int port;
    
    private final ExecutorService clientPool;

    private static final String INITIAL_FILE_NAME = "00000000000000000000";

    public MiniKafkaBroker(String topic, int partitions, int port){

        this.topic = topic;
        this.partitions = partitions;
        this.port = port;
        clientPool = Executors.newCachedThreadPool();
    }

    public void start() throws IOException {
        setupStorage();
        startServer();

    }

    private void setupStorage() throws IOException {
        System.out.printf("[*] Provisioning broker for topic '%s' with %d partitions ....%n", topic, partitions);

        for(int i = 0; i<partitions; i++){

            //create directory: e.g, "my-topic-0"
            String dirName = topic+ "-"+i;
            Path partitionDir = Paths.get(dirName);

            if(!Files.exists(partitionDir)) {
                Files.createDirectories(partitionDir);
                System.out.println("    [+] Created Directory: "+partitionDir.toAbsolutePath());
            }

            //create the 3 standard kafka files
            createFileIfNotExists(partitionDir, INITIAL_FILE_NAME+ ".log");
            createFileIfNotExists(partitionDir, INITIAL_FILE_NAME+ ".index");
            createFileIfNotExists(partitionDir, INITIAL_FILE_NAME+ ".timeindex");

        }
    }

    private void createFileIfNotExists(Path dir, String filename) throws IOException {
        Path filePath = dir.resolve(filename);
        if (!Files.exists(filePath)) {
            Files.createFile(filePath);
        }
    }

    private void startServer() throws IOException {
        try(ServerSocketChannel server = ServerSocketChannel.open()){
            server.bind(new InetSocketAddress(port));
            server.configureBlocking(true);

            System.out.println("Broker listening on "+port);

            while(true){
                try{
                    SocketChannel client = server.accept();
                    client.configureBlocking(true);

                    clientPool.submit(new ClientHandler(client));

                }catch(IOException ex){
                    System.out.println("Error accepting connection: "+ex.getMessage());
                }

            }

        }

    }

    private void saveMetadataSnapshot(){

    }

    private static class ClientHandler implements Runnable {
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
                Thread.sleep(1000);
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
