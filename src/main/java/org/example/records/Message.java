package org.example.records;

import java.io.Serializable;

public record Message(
        String command, String topic, int partition, byte[] messageBytes
) implements Serializable {
}
