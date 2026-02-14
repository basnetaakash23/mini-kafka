package org.example.component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

// This acts like a simplified "Partition" in Kafka
public class TopicLog {

    private static final String INITIAL_FILE_NAME = "00000000000000000000";

    public TopicLog(){

    }

    public LogSegment setupStorage(String topic, int partition) throws IOException {
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

            return new LogSegment(partitionDir, INITIAL_FILE_NAME);


    }

    private void createFileIfNotExists(Path dir, String filename) throws IOException {
        Path filePath = dir.resolve(filename);
        if (!Files.exists(filePath)) {
            Files.createFile(filePath);
        }
    }


}



