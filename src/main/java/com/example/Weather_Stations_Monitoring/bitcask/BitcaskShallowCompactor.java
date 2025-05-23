package com.example.Weather_Stations_Monitoring.bitcask;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Component
public class BitcaskShallowCompactor {

    @Value("${bitcask.segment.dir}")
    private String SEGMENT_DIR;

    @Value("${bitcask.hint.dir}")
    private String HINT_DIR;

    @Autowired
    private BitcaskWriter writer;

    public void cleanOldSegments() {
        System.out.println("cleanOldSegments:");
        int currentIndex = writer.getCurrentSegmentIndex();
        if (currentIndex <= 1) {
            return; // Not enough segments to clean anything yet
        }
        int keep1 = currentIndex;
        int keep2 = currentIndex - 1;

        try {
            Files.list(Paths.get(SEGMENT_DIR))
                    .filter(p -> p.getFileName().toString().startsWith("segment-"))
                    .filter(p -> p.toString().endsWith(".data"))
                    .forEach(p -> {
                        int index = extractSegmentIndex(p.getFileName().toString());
                        if (index < keep2) {
                            System.out.println("Deleting old segment: " + p.getFileName());

                            p.toFile().delete();
                        }
                    });

            Files.list(Paths.get(HINT_DIR))
                    .filter(p -> p.getFileName().toString().startsWith("segment-"))
                    .filter(p -> p.toString().endsWith(".hint"))
                    .forEach(p -> {
                        int index = extractSegmentIndex(p.getFileName().toString());
                        if (index < keep2) {
                            p.toFile().delete();
                        }
                    });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int extractSegmentIndex(String filename) {
        String numberPart = filename.replace("segment-", "").replace(".data", "").replace(".hint", "");
        return Integer.parseInt(numberPart);
    }
}

