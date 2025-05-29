package com.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.RandomAccessFile;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ParquetChangeDetector {

    private static final String ROOT_DIR = "../parquet"; // your base path
    private static final String METADATA_PATH = "processed_files.txt"; // state file
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String LOGSTASH_ENDPOINT = "http://localhost:5044";

    private static Set<String> processedFiles = new HashSet<>();
    private static final Schema SCHEMA = ReflectData.AllowNull.get().getSchema(WeatherStatus.class);

  public static void main(String[] args) {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // Load already processed files once at startup
    loadProcessedFiles();

    Runnable checkTask = () -> {
        try {
            List<File> newParquetFiles = scanForNewParquetFiles(new File(ROOT_DIR));

            for (File file : newParquetFiles) {
                readParquetFile(file.getAbsolutePath());
                processedFiles.add(file.getAbsolutePath());
            }

            saveProcessedFiles();
        } catch (Exception e) {
            e.printStackTrace();
        }
    };

    // Schedule the task to run every 1000 second
    scheduler.scheduleAtFixedRate(checkTask, 0, 1000, TimeUnit.SECONDS);
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
        File file = new File(filePath);
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(toInputFile(file))
                .withDataModel(ReflectData.get())
                .build()) {

            System.out.println("Reading: " + filePath);
            GenericRecord record;
            while ((record = reader.read()) != null) {
                String json = genericRecordToJson(record); // Convert record to JSON
                System.out.println("Sending record to Logstash: " + json);
                sendToLogstash(json);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Helper to wrap a File as a Parquet InputFile (no Hadoop)
    private static InputFile toInputFile(File file) {
        return new InputFile() {
            @Override
            public long getLength() throws IOException {
                return file.length();
            }
            @Override
            public SeekableInputStream newStream() throws IOException {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                FileChannel channel = raf.getChannel();
                return new DelegatingSeekableInputStream(new java.io.FileInputStream(raf.getFD())) {
                    @Override
                    public long getPos() throws IOException {
                        return channel.position();
                    }
                    @Override
                    public void seek(long newPos) throws IOException {
                        channel.position(newPos);
                    }
                    @Override
                    public void close() throws IOException {
                        super.close();
                        channel.close();
                        raf.close();
                    }
                };
            }
        };
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

