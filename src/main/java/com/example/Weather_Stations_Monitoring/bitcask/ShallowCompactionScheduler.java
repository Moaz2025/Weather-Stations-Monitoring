package com.example.Weather_Stations_Monitoring.bitcask;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class ShallowCompactionScheduler {

    @Autowired
    private BitcaskShallowCompactor compactor;

    @Scheduled(fixedRate = 1 * 60 * 1000) // every 1 min
    public void cleanupOldSegments() {
        System.out.println("ShallowCompactionScheduler runs at :");
        printCurrentTime();
        compactor.cleanOldSegments();
    }

    public void printCurrentTime() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d/M/yyyy HH:mm:ss");
        String formatted = now.format(formatter);
        System.out.println(formatted);
    }
}

