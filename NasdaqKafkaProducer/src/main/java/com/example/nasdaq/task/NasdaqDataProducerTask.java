
package com.example.nasdaq.task;

import com.example.nasdaq.kafka.KafkaProducer;
import com.example.nasdaq.service.NasdaqService;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class NasdaqDataProducerTask {

    private final NasdaqService nasdaqService;
    private final KafkaProducer kafkaProducer;

    @Value("${kafka.topic.name}")
    private String kafkaTopic;

    @Value("${nasdaq.stock.symbol}")
    private String stockSymbol;

    public NasdaqDataProducerTask(NasdaqService nasdaqService, KafkaProducer kafkaProducer) {
        this.nasdaqService = nasdaqService;
        this.kafkaProducer = kafkaProducer;
    }

    @Scheduled(fixedRateString = "${fetch.interval.ms}")
    public void fetchAndProduceData() {
        try {
            System.out.print("Producing message...")
            JsonNode data = nasdaqService.fetchData(stockSymbol);
            kafkaProducer.sendMessage(kafkaTopic, data.toString());
            System.out.println(" > Message sent!")

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
