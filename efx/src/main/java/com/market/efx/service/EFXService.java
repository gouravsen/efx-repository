package com.market.efx.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.market.efx.model.MarketPrice;
import org.json.CDL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class EFXService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC = "efx-message";

    ObjectMapper objectMapper = new ObjectMapper();

    public  List<MarketPrice> processCSVFile() throws JsonProcessingException {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("input.csv");
        String csvAsString = new BufferedReader(new InputStreamReader(inputStream))
                .lines()
                .collect(Collectors.joining("\n"));
        String json = CDL.toJSONArray(csvAsString).toString();
        List<MarketPrice> marketPrices = objectMapper.readValue(json,
                new TypeReference<List<MarketPrice>>() {});
        System.out.println(marketPrices);

        return marketPrices;
    }

    public void sendMessage( ) throws JsonProcessingException {
        List<MarketPrice> marketPrices = processCSVFile();

        marketPrices.stream().forEach(message ->
        {
            try {
                this.kafkaTemplate.send(TOPIC,objectMapper.writeValueAsString(message));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
