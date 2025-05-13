package com.example.Weather_Stations_Monitoring.bitcask;

import org.springframework.stereotype.Component;

@Component
public class BitcaskReader {

    public String read(String key) {
        return "Reading key: " + key;
    }

    public String readAll() {
        return "Reading All... ";
    }
}
