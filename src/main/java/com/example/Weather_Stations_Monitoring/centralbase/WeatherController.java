package com.example.Weather_Stations_Monitoring.centralbase;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/weather")
public class WeatherController {

    private final WeatherProducer weatherProducer;

    public WeatherController(WeatherProducer weatherProducer) {
        this.weatherProducer = weatherProducer;
    }

    @PostMapping("/publish")
    public ResponseEntity<String> publishWeatherStatus(@RequestBody WeatherStatus status) {
        weatherProducer.sendWeatherStatus(status);
        return ResponseEntity.ok("Weather status sent to Kafka");
    }
}

