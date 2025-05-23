package com.example.Weather_Stations_Monitoring.centralbase;

import com.example.Weather_Stations_Monitoring.bitcask.BitcaskReader;
import com.example.Weather_Stations_Monitoring.bitcask.BitcaskShallowCompactor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/bitcask")
public class BitcaskController {
    @Autowired
    BitcaskReader bitcaskReader;
    @Autowired
    private BitcaskShallowCompactor compactor;

    @GetMapping("/compact")
    public String compactNow() {
        compactor.cleanOldSegments();
        return "Compaction triggered";
    }
    @GetMapping("/view-all")
    public ResponseEntity<String> viewAll() {
        String data = bitcaskReader.readAll();
        return ResponseEntity.ok(data);
    }

    @GetMapping("/view")
    public ResponseEntity<String> viewKey(@RequestParam String key) {
        String value = bitcaskReader.read(key);
        return ResponseEntity.ok(value);
    }
}

