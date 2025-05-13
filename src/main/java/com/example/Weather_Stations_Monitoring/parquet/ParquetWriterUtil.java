package com.example.Weather_Stations_Monitoring.parquet;

import com.example.Weather_Stations_Monitoring.centralbase.WeatherStatus;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ParquetWriterUtil {

    private static final Schema SCHEMA = ReflectData.AllowNull.get().getSchema(WeatherStatus.class);

    public static void writeRecords(String dir, List<WeatherStatus> records) {
        try {
            File outDir = new File(dir);
            if (!outDir.exists()) outDir.mkdirs();

            String filename = "part-" + System.currentTimeMillis() + ".parquet";
            Path path = new Path(outDir.getAbsolutePath() + "/" + filename);

            try (ParquetWriter<WeatherStatus> writer = AvroParquetWriter.<WeatherStatus>builder(path)
                    .withSchema(SCHEMA)
                    .withDataModel(ReflectData.get())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withConf(new org.apache.hadoop.conf.Configuration())
                    .build()) {

                for (WeatherStatus record : records) {
                    writer.write(record);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
