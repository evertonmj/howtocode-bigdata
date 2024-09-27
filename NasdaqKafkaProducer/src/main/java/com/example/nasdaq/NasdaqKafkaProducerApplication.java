
package com.example.nasdaq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class NasdaqKafkaProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(NasdaqKafkaProducerApplication.class, args);
    }
}
