package org.example.service;

import org.example.component.MetadataStore;
import org.example.component.TopicLog;
import org.example.processor.CommandProcessor;
import org.example.records.broker.BrokerNode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MiniKafkaBroker {
    private final int port;
    private final ExecutorService clientPool;
    private final BrokerNode brokerNode;
    private final MetadataStore metadataStore;

    private final CommandProcessor commandProcessor;
    
    private final TopicLog topicLog;
    private static final String INITIAL_FILE_NAME = "00000000000000000000";

    public MiniKafkaBroker(int port) throws IOException {

        this.port = port;
        this.brokerNode = new BrokerNode(0, "localhost", port);
        this.metadataStore = new MetadataStore(brokerNode);
        clientPool = Executors.newCachedThreadPool();
        topicLog = new TopicLog();
        commandProcessor = new CommandProcessor(topicLog, metadataStore);
    }

    public void start() throws IOException {
        metadataStore.load();
        startServer();

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

                    clientPool.submit(new ClientHandler(client, commandProcessor));

                }catch(IOException ex){
                    System.out.println("Error accepting connection: "+ex.getMessage());
                }

            }

        }

    }

    private void saveMetadataSnapshot(){

    }


}
