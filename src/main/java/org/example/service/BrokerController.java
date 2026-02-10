package org.example.service;

import org.example.records.PartitionInfo;
import org.example.records.TopicMetadata;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerController {

    private final MetadataStore metadataStore;



    private static final ConcurrentHashMap<String, TopicLog> activeTopics = new ConcurrentHashMap<>();

    public BrokerController(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;

    }

    public TopicLog determineCommandDecision(String command, String topicName, int partitions) throws IOException {
        return null;



    }
}
