package org.example.service;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.records.BrokerNode;
import org.example.records.ClusterMetadata;
import org.example.records.PartitionInfo;
import org.example.records.TopicMetadata;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataStore {

    private static final String STORAGE_FILE = "cluster-metadata.json";

    private final BrokerNode brokerNode;

    // in memory cache : topic name - metadata object
    private Map<String, TopicMetadata> topics = new HashMap<>();
    //private final ObjectMapper objectMapper = new ObjectMapper();

    public MetadataStore(BrokerNode brokerNode){
        this.brokerNode = brokerNode;

    }

    //load the metadata
    public void load(){
//        Path partitionDir = Paths.get("/");
//        Path filePath = partitionDir.resolve(STORAGE_FILE);


        try{
            //deserialize json back to java object
            //ClusterMetadata metadata = objectMapper.readValue(filePath.toFile(), ClusterMetadata.class);
            PartitionInfo partitionInfo = new PartitionInfo(0, 1, List.of(1));
            TopicMetadata topicMetadata = new TopicMetadata("my-topic", List.of(partitionInfo));
            ClusterMetadata clusterMetadata = new ClusterMetadata(List.of(brokerNode), List.of(topicMetadata));

            //populate the internal metadata
            for(TopicMetadata topicData : clusterMetadata.topics()){
                topics.put(topicData.topicName(), topicData);
            }
            System.out.println("[*] Loaded metadata for " + topics.size() + " topics.");


        } catch(Exception ex){
            System.out.println(ex.getMessage());
        }

    }

    public boolean contains(String topicName) {
        return topics.containsKey(topicName);
    }

    public void addTopic(TopicMetadata newTopic) {
        topics.put(newTopic.topicName(), newTopic);
        save(); // Persist immediately
    }

    private void save() {
        try {
            // Convert Map -> List for storage
            ClusterMetadata snapshot = new ClusterMetadata(List.of(brokerNode), new ArrayList<>(topics.values()));
//            objectMapper.writeValue(new File(STORAGE_FILE), snapshot);
        } catch (Exception e) {
            System.err.println("CRITICAL: Failed to save metadata!");
        }
    }

    public TopicMetadata get(String topic){
        TopicMetadata meta = topics.get(topic);
        return meta == null ? null : toImmutable(meta);
    }

    private TopicMetadata toImmutable(TopicMetadata meta) {
        // return an immutable or defensively copied instance
        return new TopicMetadata(meta.topicName(), List.copyOf(meta.partitions()));
    }
}
