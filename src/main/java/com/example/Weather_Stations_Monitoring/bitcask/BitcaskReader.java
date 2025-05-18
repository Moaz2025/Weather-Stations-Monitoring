package com.example.Weather_Stations_Monitoring.bitcask;

import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Component
public class BitcaskReader {
    private final String DATA_DIR;
    private final String HINT_DIR;

    private final Map<String, ValuePosition> keyDirectory = new HashMap<>();

    public BitcaskReader(String data_dir, String hint_dir) {
        DATA_DIR = data_dir;
        HINT_DIR = hint_dir;
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
                            keyDirectory.put(key, new ValuePosition(file, offset, length));
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error loading hint files", e);
        }
    }

    public String read(String key) {
        ValuePosition pos = keyDirectory.get(key);
        if (pos == null) return "Key not found";

        try (RandomAccessFile file = new RandomAccessFile(DATA_DIR + "/" + pos.fileName(), "r")) {
            file.seek(pos.offset());
            byte[] buffer = new byte[pos.length()];
            file.readFully(buffer);
            return new String(buffer);
        } catch (IOException e) {
            return "Error reading value for key: " + key;
        }
    }

    public String readAll() {
        StringBuilder sb = new StringBuilder();
        for (String key : keyDirectory.keySet()) {
            sb.append(key).append(": ").append(read(key)).append("\n");
        }
        return sb.toString();
    }
}
