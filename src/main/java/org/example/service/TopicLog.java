package org.example.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

// This acts like a simplified "Partition" in Kafka
public class TopicLog {


    private final ReentrantLock writeLock = new ReentrantLock();
    private static final String INITIAL_FILE_NAME = "00000000000000000000";

    public TopicLog(){}

    public Path setupStorage(String topic, int partition) throws IOException {
            //create directory: e.g, "my-topic-0"
            String dirName = topic+ "-"+partition;
            Path partitionDir = Paths.get(dirName);

            if(!Files.exists(partitionDir)) {
                Files.createDirectories(partitionDir);
                System.out.println("    [+] Created Directory: "+partitionDir.toAbsolutePath());
            }

            //create the 3 standard kafka files
            createFileIfNotExists(partitionDir, INITIAL_FILE_NAME+ ".log");
            createFileIfNotExists(partitionDir, INITIAL_FILE_NAME+ ".index");
            createFileIfNotExists(partitionDir, INITIAL_FILE_NAME+ ".timeindex");


    }

    private void createFileIfNotExists(Path dir, String filename) throws IOException {
        Path filePath = dir.resolve(filename);
        if (!Files.exists(filePath)) {
            Files.createFile(filePath);
        }
    }

    public void append(byte[] message) {
        writeLock.lock(); // Ensure only one thread writes to this file at a time
        try {
            // 1. Prepare buffer (Length + Payload)
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length);
            buffer.put(message);
            buffer.flip();

            // 2. Write to end of file
            fileChannel.write(buffer, fileChannel.size());

            // In a real system, you might not force() on every write for speed (page cache)
            // fileChannel.force(false);
        } catch (IOException e) {
            e.printStackTrace(); // Handle disk errors gracefully
        } finally {
            writeLock.unlock();
        }
    }
}



