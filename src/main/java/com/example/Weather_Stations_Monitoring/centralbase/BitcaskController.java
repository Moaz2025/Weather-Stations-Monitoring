package com.example.Weather_Stations_Monitoring.centralbase;

import com.example.Weather_Stations_Monitoring.bitcask.BitcaskReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/bitcask")
public class BitcaskController {
    @Autowired
    BitcaskReader bitcaskReader;

    @GetMapping("/view-all")
    public void viewAll() {
        bitcaskReader.readAll();
    }

    @GetMapping("/view")
    public String viewKey(@RequestParam String key) {
        return bitcaskReader.read(key);
    }
}

