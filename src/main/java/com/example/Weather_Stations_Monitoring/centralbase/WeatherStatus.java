package com.example.Weather_Stations_Monitoring.centralbase;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WeatherStatus {
    private long stationId;
    private long sNo;
    private String batteryStatus;
    private long statusTimestamp;
    private Weather weather;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Weather {
        private int humidity;
        private int temperature;
        private int windSpeed;
    }
}
