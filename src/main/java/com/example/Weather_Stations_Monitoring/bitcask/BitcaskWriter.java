package com.example.Weather_Stations_Monitoring.bitcask;

import com.example.Weather_Stations_Monitoring.centralbase.WeatherStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

@Component
public class BitcaskWriter {
//    private final InMemoryIndex index = new InMemoryIndex();
    private final RandomAccessFile file = new RandomAccessFile("data/bitcask/data.log", "rw");

    public BitcaskWriter() throws FileNotFoundException {
    }

    public synchronized void write(WeatherStatus weatherStatus) throws IOException {
        long offset = file.length();
        String json = new ObjectMapper().writeValueAsString(weatherStatus);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        file.seek(offset);
        file.write(bytes);
//        index.update(String.valueOf(status.getStationId()), offset);
    }
}

