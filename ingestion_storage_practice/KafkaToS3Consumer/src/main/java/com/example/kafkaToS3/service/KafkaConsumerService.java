
package com.example.kafkaToS3.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final S3Client s3Client;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    public KafkaConsumerService(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "group_id")
    public void consume(List<ConsumerRecord<String, String>> messages) throws IOException {
        logger.info("Received {} messages from Kafka", messages.size());

        // Create CSV file locally
        String fileName = "weather_data.csv";
        try (FileWriter csvWriter = new FileWriter(fileName)) {
            csvWriter.append("temperature,windspeed,winddirection,time");
            for (ConsumerRecord<String, String> message : messages) {
                csvWriter.append(message.value()).append("");
            }
        }

        // Upload CSV to S3
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build(), Paths.get(fileName));

        logger.info("Uploaded file {} to S3 bucket {}", fileName, bucketName);

        // Optionally, delete messages from topic (depends on Kafka config, you can manage offsets)
    }
}
