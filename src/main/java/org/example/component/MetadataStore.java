package org.example.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.records.broker.BrokerNode;
import org.example.records.broker.ClusterMetadata;
import org.example.records.broker.PartitionInfo;
import org.example.records.broker.TopicMetadata;

import java.io.IOException;
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

    // Created the object mapper static final as the object mapper might be required multiple times during the application runtime and hence a single instance will
    // be in application memory throughout the application runtime.
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public MetadataStore(BrokerNode brokerNode){
        this.brokerNode = brokerNode;

    }


    //made it static, because it runs only once and is used only once.
    public static ClusterMetadata readClusterMetadata() throws IOException {
        Path filePath = Paths.get("metadata/cluster-metadata.json");
        ClusterMetadata metadata = objectMapper.readValue(filePath.toFile(), ClusterMetadata.class);
        return metadata;
    }



    //load the metadata
    public void load(){

        try{
            //deserialize json back to java object
            ClusterMetadata clusterMetadata = MetadataStore.readClusterMetadata();

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
