package org.example.records.broker;

import java.io.Serializable;
import java.util.List;

public record PartitionInfo(
        int partitionId,
        int leaderBrokerId,  // The broker acting as LEADER (Reads/Writes)
        List<Integer> replicaBrokerIds // List of followers (for fault tolerance)
) implements Serializable {
}
