package com.example.Weather_Stations_Monitoring.parquet;

import com.example.Weather_Stations_Monitoring.centralbase.WeatherStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class ParquetBatchWriter {

    private final List<WeatherStatus> buffer = new ArrayList<>();
    private static final int BATCH_SIZE = 10000;
    private static final String BASE_DIR = "src/main/resources/data/parquet/";

    public synchronized void add(WeatherStatus status) throws IOException {
        buffer.add(status);
        if (buffer.size() >= BATCH_SIZE) {
            flush();
        }
    }

    private void flush() throws IOException {
        Map<String, List<WeatherStatus>> grouped = buffer.stream()
                .collect(Collectors.groupingBy(this::partitionPath));

        for (Map.Entry<String, List<WeatherStatus>> entry : grouped.entrySet()) {
            String path = entry.getKey();
            List<WeatherStatus> records = entry.getValue();

            ParquetWriterUtil.writeRecords(path, records);
        }

        buffer.clear();
    }

    private String partitionPath(WeatherStatus status) {
        ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of("UTC"));
        return BASE_DIR +
                "station_id=" + status.getStationId() + "/" +
                "year=" + zonedDateTime.getYear() + "/" +
                "month=" + String.format("%02d", zonedDateTime.getMonthValue()) + "/" +
                "day=" + String.format("%02d", zonedDateTime.getDayOfMonth()) + "/" +
                "hour=" + String.format("%02d", zonedDateTime.getHour()) + "/";
    }
}
