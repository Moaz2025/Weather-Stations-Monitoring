package com.example.Weather_Stations_Monitoring.centralbase;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WeatherStatus {
    private long stationId;
    @JsonProperty("sNo")
    private long sNo;
    private String batteryStatus;
    private long statusTimestamp;
    private WeatherData weather;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WeatherData {
        private int humidity;
        private int temperature;
        private int windSpeed;
    }
}
