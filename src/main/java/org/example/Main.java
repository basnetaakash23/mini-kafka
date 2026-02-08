package org.example;

import org.example.service.MiniKafkaBroker;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        if(args.length < 3){
            System.out.println("Usage: java KafkaBroker <topic> <partitions> <port>");
            System.out.println("Example: java KafkaBroker my-topic 3 9092");
            System.exit(1);
        }

        String topic = args[0];
        int partitions = Integer.parseInt(args[1]);
        int port = Integer.parseInt(args[2]);

        MiniKafkaBroker miniKafkaBroker = new MiniKafkaBroker( port);
        miniKafkaBroker.start();

    }
}