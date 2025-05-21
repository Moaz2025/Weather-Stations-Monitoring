package com.example.Weather_Stations_Monitoring.bitcask;

import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.util.*;

@Component
public class BitcaskWriter {

    private final String SEGMENT_DIR;
    private final String HINT_DIR;
    private static final long MAX_SEGMENT_SIZE = 1024 * 1024; // 1MB

    private final Map<String, ValuePosition> keyDirectory = new HashMap<>();
    private RandomAccessFile currentSegmentFile;
    private BufferedWriter currentHintWriter;
    private String currentSegmentName;
    private long currentOffset = 0;

    public BitcaskWriter(String segment_dir, String hint_dir) {
        SEGMENT_DIR = segment_dir;
        HINT_DIR = hint_dir;
        try {
            Files.createDirectories(Paths.get(SEGMENT_DIR));
            Files.createDirectories(Paths.get(HINT_DIR));
            initSegment();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize Bitcask directories", e);
        }
    }

    private void initSegment() throws IOException {
        int segmentIndex = getNextSegmentIndex();
        currentSegmentName = "segment-" + segmentIndex + ".data";
        String segmentPath = SEGMENT_DIR + "/" + currentSegmentName;
        currentSegmentFile = new RandomAccessFile(segmentPath, "rw");
        currentSegmentFile.seek(currentSegmentFile.length());
        currentOffset = currentSegmentFile.getFilePointer();

        String hintPath = HINT_DIR + "/segment-" + segmentIndex + ".hint";
        currentHintWriter = new BufferedWriter(new FileWriter(hintPath, true));
    }

    private int getNextSegmentIndex() throws IOException {
        File folder = new File(SEGMENT_DIR);
        String[] files = folder.list((dir, name) -> name.endsWith(".data"));
        if (files == null || files.length == 0) return 1;

        return Arrays.stream(files)
                .map(name -> name.replace("segment-", "").replace(".data", ""))
                .mapToInt(Integer::parseInt)
                .max()
                .orElse(0) + 1;
    }

    public synchronized void write(String key, String value) {
        try {
            byte[] keyBytes = key.getBytes();
            byte[] valueBytes = value.getBytes();

            int entrySize = 4 + keyBytes.length + 4 + valueBytes.length;
            if (currentOffset + entrySize > MAX_SEGMENT_SIZE) {
                rotateSegment();
            }

            // Write format: [keyLen][key][valueLen][value]
            currentSegmentFile.writeInt(keyBytes.length);
            currentSegmentFile.write(keyBytes);
            currentSegmentFile.writeInt(valueBytes.length);
            currentSegmentFile.write(valueBytes);

            ValuePosition pos = new ValuePosition(currentSegmentName, currentOffset, valueBytes.length);
            keyDirectory.put(key, pos);

            // Append to hint file
            currentHintWriter.write(key + "," + currentSegmentName + "," + currentOffset + "," + valueBytes.length);
            currentHintWriter.newLine();
            currentHintWriter.flush();

            currentOffset += entrySize;

        } catch (IOException e) {
            throw new RuntimeException("Failed to write key: " + key, e);
        }
    }

    private void rotateSegment() throws IOException {
        currentSegmentFile.close();
        currentHintWriter.close();
        initSegment();
    }
}
