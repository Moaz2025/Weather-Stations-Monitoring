package com.example.Weather_Stations_Monitoring.parquet;

import com.example.Weather_Stations_Monitoring.centralbase.WeatherStatus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class ParquetWriterUtil {

    private static final String AVRO_SCHEMA = """
        {
          "type": "record",
          "name": "WeatherStatus",
          "fields": [
            {"name": "stationId", "type": "long"},
            {"name": "sNo", "type": "long"},
            {"name": "batteryStatus", "type": "string"},
            {"name": "statusTimestamp", "type": "long"},
            {
              "name": "weather",
              "type": {
                "type": "record",
                "name": "WeatherData",
                "fields": [
                  {"name": "humidity", "type": "int"},
                  {"name": "temperature", "type": "int"},
                  {"name": "windSpeed", "type": "int"}
                ]
              }
            }
          ]
        }
        """;

    public static void writeRecords(String outputPath, List<WeatherStatus> records) throws IOException {
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
        Schema weatherSchema = schema.getField("weather").schema();

        String filename = System.currentTimeMillis() + ".parquet"; // or use a formatted timestamp
        File file = new File(outputPath, filename);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            if (!parent.mkdirs()) {
                throw new IOException("Failed to create directories: " + parent.getAbsolutePath());
            }
        }
        if (file.exists()) {
            file.delete();
        }

        OutputFile outputFile = new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) throws IOException {
                return new PositionOutputStream() {
                    private final FileOutputStream out = new FileOutputStream(file);
                    private long position = 0;

                    @Override
                    public void write(int b) throws IOException {
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public long getPos() throws IOException {
                        return position;
                    }

                    @Override
                    public void close() throws IOException {
                        out.close();
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };

        try (var writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (WeatherStatus status : records) {
                GenericRecord weatherRecord = new GenericData.Record(weatherSchema);
                weatherRecord.put("humidity", status.getWeather().getHumidity());
                weatherRecord.put("temperature", status.getWeather().getTemperature());
                weatherRecord.put("windSpeed", status.getWeather().getWindSpeed());

                GenericRecord record = new GenericData.Record(schema);
                record.put("stationId", status.getStationId());
                record.put("sNo", status.getSNo());
                record.put("batteryStatus", status.getBatteryStatus());
                record.put("statusTimestamp", status.getStatusTimestamp());
                record.put("weather", weatherRecord);

                writer.write(record);
            }
        }
    }
}
