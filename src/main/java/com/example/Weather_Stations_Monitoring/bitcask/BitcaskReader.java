package com.example.Weather_Stations_Monitoring.bitcask;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;

@Component
public class BitcaskReader {

    private final String DATA_DIR;
    private final String HINT_DIR;

    private final BitcaskIndex bitcaskIndex;

    @Autowired
    public BitcaskReader(@Value("${bitcask.segment.dir}") String segment_dir,
                         @Value("${bitcask.hint.dir}") String hint_dir,
                         BitcaskIndex bitcaskIndex) {
        this.DATA_DIR = segment_dir;
        this.HINT_DIR = hint_dir;
        this.bitcaskIndex = bitcaskIndex;
        loadHintFiles();
    }

    // Load hint files to reconstruct in-memory index
    private void loadHintFiles() {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(HINT_DIR))) {
            for (Path path : stream) {
                try (BufferedReader reader = Files.newBufferedReader(path)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split(",");
                        if (parts.length == 4) {
                            String key = parts[0];
                            String file = parts[1];
                            long offset = Long.parseLong(parts[2]);
                            int length = Integer.parseInt(parts[3]);
                            bitcaskIndex.put(key, new ValuePosition(file, offset, length));
                        }
                    }
                }
            }
        } catch (IOException e) {
            //System.err.println("Error loading hint files: " + e.getMessage());
        }
    }

    public String read(String key) {
        ValuePosition pos = bitcaskIndex.get(key);
        //System.out.println("ValPos: " + pos);
        if (pos == null) return "Key not found";

        try (RandomAccessFile file = new RandomAccessFile(DATA_DIR + "/" + pos.fileName(), "r")) {
            file.seek(pos.offset());
            int valueLength = file.readInt();
            byte[] valueBytes = new byte[valueLength];
            file.readFully(valueBytes);
            return new String(valueBytes);

        } catch (IOException e) {
            return "Error reading value for key: " + key;
        }
    }

    public String readAll() {
        StringBuilder sb = new StringBuilder();
        //System.out.println("Number of messages: " + bitcaskIndex.getAll().keySet().size());
        for (String key : bitcaskIndex.getAll().keySet()) {
            sb.append(key).append(": ").append(read(key)).append("\n");
        }
        return sb.toString();
    }
}
