
package com.example.kafkakinesis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.core.SdkBytes;

import java.nio.ByteBuffer;

@Service
public class KafkaConsumerService {

    private final KinesisClient kinesisClient;
    private final ObjectMapper objectMapper;

    public KafkaConsumerService(KinesisClient kinesisClient, ObjectMapper objectMapper) {
        this.kinesisClient = kinesisClient;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "market_ticker_data", groupId = "market_ticker_group")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            JsonNode message = objectMapper.readTree(record.value());
            System.out.println("Received message: " + message);

            // Send to Kinesis
            PutRecordRequest request = PutRecordRequest.builder()
                    .streamName("your_kinesis_stream_name")
                    .data(SdkBytes.fromByteBuffer(ByteBuffer.wrap(record.value().getBytes())))
                    .partitionKey("partitionKey")
                    .build();

            PutRecordResponse response = kinesisClient.putRecord(request);
            System.out.println("Message sent to Kinesis: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
