package com.example.Weather_Stations_Monitoring.centralbase;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class WeatherProducer {

    private final KafkaTemplate<String, WeatherStatus> kafkaTemplate;

    public WeatherProducer(KafkaTemplate<String, WeatherStatus> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendWeatherStatus(WeatherStatus weatherStatus) {
        String topic = "weather-topic";
        kafkaTemplate.send(topic, String.valueOf(weatherStatus.getStationId()), weatherStatus);
    }
}

