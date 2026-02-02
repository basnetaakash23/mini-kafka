package org.example.records;

import java.io.Serializable;
import java.util.List;

public record TopicMetadata(
        String topicName,
        List<PartitionInfo> partitions) implements Serializable {
}
