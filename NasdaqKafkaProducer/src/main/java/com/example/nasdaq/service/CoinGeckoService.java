package com.example.nasdaq.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class CoinGeckoService {

    @Value("${coingecko.api.url}")
    private String apiUrl;

    @Value("${coingecko.currency}")
    private String currency;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public CoinGeckoService(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    public JsonNode fetchCryptoData() throws Exception {
        String url = String.format("%s?vs_currency=%s", apiUrl, currency);
        String response = restTemplate.getForObject(url, String.class);
        return objectMapper.readTree(response);
    }
}
