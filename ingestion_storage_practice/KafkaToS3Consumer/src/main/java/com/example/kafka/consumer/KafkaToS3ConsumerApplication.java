
package com.example.kafka.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaToS3ConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaToS3ConsumerApplication.class, args);
    }
}
