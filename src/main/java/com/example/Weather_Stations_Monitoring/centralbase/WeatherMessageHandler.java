package com.example.Weather_Stations_Monitoring.centralbase;

import com.example.Weather_Stations_Monitoring.bitcask.BitcaskWriter;
import com.example.Weather_Stations_Monitoring.parquet.ParquetBatchWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class WeatherMessageHandler {
    @Autowired
    private BitcaskWriter bitcaskWriter;
    @Autowired
    private ParquetBatchWriter parquetBatchWriter;
    @Autowired
    private ObjectMapper mapper;

    public void handle(String json) {
        try {
            WeatherStatus weatherStatus = mapper.readValue(json, WeatherStatus.class);
            bitcaskWriter.write(weatherStatus);
            parquetBatchWriter.add(weatherStatus);
            System.out.println(weatherStatus);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

