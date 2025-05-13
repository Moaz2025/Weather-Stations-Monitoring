package com.example.Weather_Stations_Monitoring.centralbase;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    @Autowired
    private WeatherMessageHandler weatherMessageHandler;

    @KafkaListener(topics = "weather-topic", groupId = "central-station")
    public void listen(String json) {
        weatherMessageHandler.handle(json);
        System.out.println(json);
    }
}

