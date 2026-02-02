package org.example.records;

import java.io.Serializable;
import java.util.List;

public record ClusterMetadata(
        List<BrokerNode> brokers,
        List<TopicMetadata> topics
) implements Serializable {
}
