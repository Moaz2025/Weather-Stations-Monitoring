package com.example.Weather_Stations_Monitoring.parquet;

import com.example.Weather_Stations_Monitoring.centralbase.WeatherStatus;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;
import java.util.List;

public class ParquetWriterUtil {

    private static final Schema SCHEMA = ReflectData.AllowNull.get().getSchema(WeatherStatus.class);

    public static void writeRecords(String dir, List<WeatherStatus> records) {
        try {
            File outDir = new File(dir);
            if (!outDir.exists()) outDir.mkdirs();

            String filename = "part-" + System.currentTimeMillis() + ".parquet";
            File file = new File(outDir, filename);

            OutputFile outputFile = new LocalOutputFile(file);

            try (var writer = AvroParquetWriter.<WeatherStatus>builder(outputFile)
                    .withSchema(SCHEMA)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withDataModel(ReflectData.get())
                    .build()) {

                for (WeatherStatus record : records) {
                    writer.write(record);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Custom OutputFile implementation
    static class LocalOutputFile implements OutputFile {
        private final File file;

        public LocalOutputFile(File file) {
            this.file = file;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return new LocalPositionOutputStream(new FileOutputStream(file));
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
    }

    // Local implementation of PositionOutputStream
    static class LocalPositionOutputStream extends DelegatingPositionOutputStream {
        private long position = 0;

        public LocalPositionOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public long getPos() throws IOException {
            return position;
        }

        @Override
        public void write(int b) throws IOException {
            super.write(b);
            position++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            super.write(b, off, len);
            position += len;
        }
    }
}
