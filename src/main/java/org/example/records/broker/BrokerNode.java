package org.example.records.broker;

import java.io.Serializable;

public record BrokerNode(
        int id,
        String host,
        int port
) implements Serializable {
}
