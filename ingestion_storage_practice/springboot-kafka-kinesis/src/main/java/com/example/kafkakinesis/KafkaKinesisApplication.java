
package com.example.kafkakinesis;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.core.SdkBytes;

@SpringBootApplication
public class KafkaKinesisApplication {

    @Value("${kinesis.stream.name}")
    private String kinesisStreamName;

    private final KinesisClient kinesisClient;

    public KafkaKinesisApplication(KinesisClient kinesisClient) {
        this.kinesisClient = kinesisClient;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaKinesisApplication.class, args);
    }

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${kafka.group.id}")
    public void consume(String message) {
        PutRecordRequest request = PutRecordRequest.builder()
            .streamName(kinesisStreamName)
            .partitionKey("partitionKey")
            .data(SdkBytes.fromUtf8String(message))
            .build();

        kinesisClient.putRecord(request);
    }
}
