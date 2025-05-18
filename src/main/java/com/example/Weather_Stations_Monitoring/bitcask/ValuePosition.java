package com.example.Weather_Stations_Monitoring.bitcask;

public record ValuePosition(
        String fileName,
        long offset,
        int length
) {}
