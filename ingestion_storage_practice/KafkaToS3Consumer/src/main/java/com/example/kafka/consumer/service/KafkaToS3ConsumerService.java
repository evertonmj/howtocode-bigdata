
package com.example.kafka.consumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Service
public class KafkaToS3ConsumerService {

    @Value("${aws.s3.bucket.name}")
    private String bucketName;

    @Value("${aws.s3.region}")
    private String region;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final S3Client s3Client;

    public KafkaToS3ConsumerService() {
        Region awsRegion = Region.of(region);
        this.s3Client = S3Client.builder()
                .region(awsRegion)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeMessages(List<ConsumerRecord<String, String>> records) throws Exception {
        List<JsonNode> messages = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            JsonNode jsonMessage = objectMapper.readTree(record.value());
            messages.add(jsonMessage);
        }

        // Write messages to a CSV file
        String csvFilePath = "weather_data.csv";
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(csvFilePath));
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Temperature", "Windspeed", "Winddirection", "Time"))) {
            for (JsonNode message : messages) {
                csvPrinter.printRecord(message.get("temperature").asDouble(),
                        message.get("windspeed").asDouble(),
                        message.get("winddirection").asDouble(),
                        message.get("time").asText());
            }
        }

        // Upload CSV file to S3
        uploadFileToS3(csvFilePath);
    }

    private void uploadFileToS3(String filePath) throws IOException {
        File file = new File(filePath);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(file.getName())
                .build();
        s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));
        System.out.println("File uploaded to S3: " + filePath);
        Files.deleteIfExists(Paths.get(filePath)); // Delete the local CSV file after upload
    }
}
