
package com.example.nasdaq.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class NasdaqService {
    @Value("${nasdaq.api.url}")
    private String apiUrl;

    @Value("${nasdaq.api.key}")
    private String apiKey;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public NasdaqService(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    public JsonNode fetchData(String stockSymbol) throws Exception {
        String url = apiUrl;
        String response = restTemplate.getForObject(url, String.class);
        return objectMapper.readTree(response);
    }
}
