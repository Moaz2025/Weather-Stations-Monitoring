package com.example.Weather_Stations_Monitoring.bitcask;


import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

@Component
public class BitcaskIndex {

    private final Map<String, ValuePosition> keyDirectory = new ConcurrentHashMap<>();

    public void put(String key, ValuePosition position) {
        keyDirectory.put(key, position);
    }

    public ValuePosition get(String key) {
        return keyDirectory.get(key);
    }

    public Map<String, ValuePosition> getAll() {
        return keyDirectory;
    }

    public void clear() {
        keyDirectory.clear();
    }

}

