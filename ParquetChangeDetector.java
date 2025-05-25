package com.example;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class ParquetChangeDetector {

    private static final String ROOT_DIR = "../parquet"; // your base path
    private static final String METADATA_PATH = "processed_files.txt"; // state file
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String LOGSTASH_ENDPOINT = "http://localhost:5044"; 

    private static Set<String> processedFiles = new HashSet<>();
    private static final Schema SCHEMA = ReflectData.AllowNull.get().getSchema(WeatherStatus.class);

    public static void main(String[] args) throws IOException {
        loadProcessedFiles();

        List<File> newParquetFiles = scanForNewParquetFiles(new File(ROOT_DIR));

        for (File file : newParquetFiles) {
            readParquetFile(file.getAbsolutePath());
            processedFiles.add(file.getAbsolutePath());
        }

        saveProcessedFiles();
    }

    private static List<File> scanForNewParquetFiles(File root) {
        List<File> newFiles = new ArrayList<>();

        Queue<File> queue = new LinkedList<>();
        queue.add(root);

        while (!queue.isEmpty()) {
            File current = queue.poll();
            if (current.isDirectory()) {
                File[] children = current.listFiles();
                if (children != null) {
                    queue.addAll(Arrays.asList(children));
                }
            } else if (current.getName().endsWith(".parquet") && !processedFiles.contains(current.getAbsolutePath())) {
                newFiles.add(current);
            }
        }

        return newFiles;
    }
    private static String genericRecordToJson(GenericRecord record) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    Encoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
    writer.write(record, encoder);
    encoder.flush();
    return out.toString("UTF-8");
}

    private static void readParquetFile(String filePath) {
        // Commented out actual Parquet reading
        
        try {
            Path parquetFilePath = new Path(filePath);
            Configuration conf = new Configuration();
            conf.set("parquet.avro.schema", SCHEMA.toString());
            conf.set(AvroReadSupport.AVRO_DATA_SUPPLIER, "org.apache.parquet.avro.ReflectDataSupplier");

            ParquetReader<Object> reader = ParquetReader.builder(new AvroReadSupport<>(), parquetFilePath)
                    .withConf(conf)
                    .build();

            Object record;
            System.out.println("Reading: " + filePath);
            while ((record = reader.read()) != null) {
    if (record instanceof GenericRecord genericRecord) {
        String json = genericRecordToJson(genericRecord);
        System.out.println("Sending record to Logstash: " + json);
        sendToLogstash(json);
    }
}

        } catch (IOException e) {
            e.printStackTrace();
        }
        

        // Create dummy WeatherStatus and send as JSON to Logstash
        // WeatherStatus.Weather weather = new WeatherStatus.Weather(55, 22, 10);
        // WeatherStatus status = new WeatherStatus(123L, 456L, "Good", System.currentTimeMillis(), weather);
        // try {
        //     String json = mapper.writeValueAsString(status);
        //     System.out.println("Sending dummy WeatherStatus to Logstash: " + json);
        //     sendToLogstash(json);
        // } catch (IOException e) {
        //     e.printStackTrace();
        // }
    }
     private static void sendToLogstash(String json) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(LOGSTASH_ENDPOINT);
            post.setEntity(new StringEntity(json));
            post.setHeader("Content-type", "application/json");

            client.execute(post);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void loadProcessedFiles() {
        File file = new File(METADATA_PATH);
        if (!file.exists()) return;

        try {
            List<String> lines = Files.readAllLines(file.toPath());
            processedFiles.addAll(lines);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void saveProcessedFiles() {
        try {
            Files.write(Paths.get(METADATA_PATH), processedFiles);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

