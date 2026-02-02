package org.example.records;

import java.io.Serializable;

public record BrokerNode(
        int id,
        String host,
        int port
) implements Serializable {
}
