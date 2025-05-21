package com.example.Weather_Stations_Monitoring.centralbase;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    @Autowired
    private WeatherMessageHandler weatherMessageHandler;

    @KafkaListener(topics = "weather-data", groupId = "central-station")
    public void listen(String json) {
        //System.out.println("Received json at consumer " + json);
        weatherMessageHandler.handle(json);
    }
}

