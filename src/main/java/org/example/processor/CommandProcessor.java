package org.example.processor;

import org.example.component.MetadataStore;
import org.example.records.Message;
import org.example.component.LogSegment;
import org.example.component.TopicLog;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CommandProcessor {

    private final Map<String, LogSegment> activeTopics = new ConcurrentHashMap<>();
    private final TopicLog topicLog;

    private final MetadataStore metadataStore;

    public CommandProcessor(TopicLog topicLog, MetadataStore metadataStore) {
        this.topicLog = topicLog;
        this.metadataStore = metadataStore;
    }

    public void parseCommand(Message message) throws IOException {

        // 2. Handle CREATE-TOPIC
        if (message.command().equals("CREATE-TOPIC")) {
            // Create the topic log and store it in the map
            if(metadataStore.contains(message.topic())){
                System.out.println("TOPIC = "+message.topic()+" already exists.");
                return;
            }
            activeTopics.put(message.topic(), topicLog.setupStorage(message.topic(), message.partition()));
            System.out.println("Topic created: " + message.topic());
        }

        // 3. Handle PRODUCE
        else if (message.command().equals("PRODUCE")) {
            // Retrieve the EXISTING topic from the map
            LogSegment logSegment = activeTopics.get(message.topic());

            if (topicLog != null) {
                // Delegate writing to the TopicLog (which finds the correct LogSegment)
                logSegment.append(message.messageBytes());
            } else {
                throw new IOException("Topic not found: " + message.topic());
            }
        }

        // 4. Handle CONSUME
        else if (message.command().equals("CONSUME")) {
//            TopicLog topicLog = activeTopics.get(topicName);
//            if (topicLog != null) {
//                // topicLog.read(...)
//            }
        }
    }


}
