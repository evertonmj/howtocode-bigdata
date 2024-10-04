
package com.example.kafkakinesis;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

@Configuration
public class KinesisClientConfig {

    @Bean
    public KinesisClient kinesisClient() {
        return KinesisClient.builder()
                .region(Region.US_EAST_1) // You can change the region as needed
                .build();
    }
}
