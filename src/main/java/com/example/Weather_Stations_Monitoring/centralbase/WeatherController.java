package com.example.Weather_Stations_Monitoring.centralbase;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
@RequestMapping("/api/weather")
public class WeatherController {

    private final WeatherProducer weatherProducer;

    public WeatherController(WeatherProducer weatherProducer) {
        this.weatherProducer = weatherProducer;
    }

    private String randomBatteryStatus(Random random) {
        int value = random.nextInt(100);
        if (value < 30) return "low";
        else if (value < 70) return "medium";
        else return "high";
    }

    @PostMapping("/publish")
    public ResponseEntity<String> publishWeatherStatus(@RequestBody WeatherStatus weatherStatus) {
        //System.out.println("Received json at controller " + weatherStatus);
        weatherProducer.sendWeatherStatus(weatherStatus);
        return ResponseEntity.ok("Weather status sent to Kafka");
    }

    @PostMapping("/test")
    public ResponseEntity<String> publishWeatherStatus(@RequestParam int count) {
        Map<Long, Long> stationCounters = new HashMap<>();

        Random random = new Random();

        for (int i = 0; i < count; i++) {
            long stationId = 1 + random.nextInt(10);

            long sNo = stationCounters.getOrDefault(stationId, 0L) + 1;
            stationCounters.put(stationId, sNo);

            String batteryStatus = randomBatteryStatus(random);

            WeatherStatus.Weather weather = new WeatherStatus.Weather(
                    10 + random.nextInt(91),
                    30 + random.nextInt(71),
                    1 + random.nextInt(100)
            );

            WeatherStatus status = new WeatherStatus(
                    stationId,
                    sNo,
                    batteryStatus,
                    System.currentTimeMillis(),
                    weather
            );

            weatherProducer.sendWeatherStatus(status);
        }

        return ResponseEntity.ok(count + " Weather status messages sent to Kafka");
    }

}

