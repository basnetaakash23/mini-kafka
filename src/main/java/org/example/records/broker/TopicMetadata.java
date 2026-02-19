package org.example.records.broker;

import org.example.records.broker.PartitionInfo;

import java.io.Serializable;
import java.util.List;

public record TopicMetadata(
        String topicName,
        List<PartitionInfo> partitions) implements Serializable {
}
