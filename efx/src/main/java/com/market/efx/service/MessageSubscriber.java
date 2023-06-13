package com.market.efx.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.market.efx.model.MarketPrice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class MessageSubscriber {

    private final static String bootstrapServers = "localhost:9092";
    private final static String groupId = "group-id";
    private final static String topic = "efx-message";

    private final static String updateTopic = "efx-update";

    ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private CSVUtil csvUtil;

    public static Properties properties;
    static {
        properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer .class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer .class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    }

    //@KafkaListener(topics = "efx-update", groupId = "group-id", autoStartup = "false")
    public void consumeUpdatedEFX() throws JsonProcessingException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        List<TopicPartition> partitions = consumer.partitionsFor(updateTopic).stream().map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toList());
        consumer.assign(partitions);
        consumer.seekToEnd(Collections.emptySet());
        Map<TopicPartition, Long> endPartitions = partitions.stream().collect(Collectors.toMap(Function.identity(), consumer::position));
        Long numberOfMessages = partitions.stream().mapToLong(p -> endPartitions.get(p)).sum();

        System.out.println(numberOfMessages);
    }

    public void getALlTopics() throws IOException {

        // create consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(3000));
            for (ConsumerRecord<String, String> record : records) {
                MarketPrice marketPrice = objectMapper.readValue(record.value(), MarketPrice.class);
                System.out.println(processMarketPrice(marketPrice));
                csvUtil.CSVWriter(processMarketPrice(marketPrice));
            }
        }
    }

    public MarketPrice processMarketPrice(MarketPrice marketPrice){
        Double bid = marketPrice.getBid();
        bid = bid - (bid*0.1/100);
        marketPrice.setBid(bid);
        Double ask = marketPrice.getAsk();
        ask = ask + (ask*0.1/100);
        marketPrice.setAsk(ask);
        return marketPrice;
    }

}
