package com.example.nasdaq.task;

import com.example.nasdaq.kafka.KafkaProducer;
import com.example.nasdaq.service.CoinGeckoService;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class CoinGeckoDataProducerTask implements CommandLineRunner {

    private final CoinGeckoService coinGeckoService;
    private final KafkaProducer kafkaProducer;

    @Value("${kafka.topic.name}")
    private String kafkaTopic;

    public CoinGeckoDataProducerTask(CoinGeckoService coinGeckoService, KafkaProducer kafkaProducer) {
        this.coinGeckoService = coinGeckoService;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void run(String... args) throws Exception {
        fetchAndProduceData();
    }

    @Scheduled(fixedRateString = "${fetch.interval.ms}")
    public void fetchAndProduceData() {
        try {
            // Fetch crypto data (a list of market information for various coins)
            JsonNode cryptoDataArray = coinGeckoService.fetchCryptoData();

            // Loop through each cryptocurrency and send singular messages
            for (JsonNode cryptoData : cryptoDataArray) {
                String message = convertCryptoDataToMessage(cryptoData);
                kafkaProducer.sendMessage(kafkaTopic, message);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Method to convert the fetched crypto data into a singular message
    private String convertCryptoDataToMessage(JsonNode cryptoData) {
        String id = cryptoData.get("id").asText();
        String symbol = cryptoData.get("symbol").asText();
        String name = cryptoData.get("name").asText();
        double currentPrice = cryptoData.get("current_price").asDouble();
        double marketCap = cryptoData.get("market_cap").asDouble();
        double priceChangePercentage24h = cryptoData.get("price_change_percentage_24h").asDouble();

        return String.format(
            "{\"id\": \"%s\", \"symbol\": \"%s\", \"name\": \"%s\", \"current_price\": %.2f, \"market_cap\": %.2f, \"price_change_percentage_24h\": %.2f}",
            id, symbol, name, currentPrice, marketCap, priceChangePercentage24h
        );
    }
}
