
package com.example.weather.task;

import com.example.weather.kafka.KafkaProducer;
import com.example.weather.service.WeatherService;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class WeatherDataProducerTask implements CommandLineRunner {

    private final WeatherService weatherService;
    private final KafkaProducer kafkaProducer;

    @Value("${kafka.topic.name}")
    private String kafkaTopic;

    public WeatherDataProducerTask(WeatherService weatherService, KafkaProducer kafkaProducer) {
        this.weatherService = weatherService;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void run(String... args) throws Exception {
        // Start producing data on application startup
        fetchAndProduceData();
    }

    @Scheduled(fixedRateString = "${fetch.interval.ms}")
    public void fetchAndProduceData() {
        try {
            // Fetch weather data from the Open-Meteo API
            JsonNode weatherData = weatherService.fetchWeatherData();

            // Convert data to a string message
            String message = convertWeatherDataToMessage(weatherData);
            kafkaProducer.sendMessage(kafkaTopic, message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Method to convert the fetched weather data into a message format
    private String convertWeatherDataToMessage(JsonNode weatherData) {
        double temperature = weatherData.get("temperature").asDouble();
        double windspeed = weatherData.get("windspeed").asDouble();
        String time = weatherData.get("time").asText();

        return String.format("{"temperature": %.2f, "windspeed": %.2f, "time": "%s"}", temperature, windspeed, time);
    }
}
