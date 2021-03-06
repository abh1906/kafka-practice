package com.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        String bootstrapServers="127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"myConsumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); // Latest if want to read from latest message , earliest if want to read from the very beginning
        // create consumer
        KafkaConsumer<String,String> consumer =  new KafkaConsumer<String, String>(properties);

        //subscribe a topic
        consumer.subscribe(Arrays.asList("myTopic"));

        // poll for new data
        while(true){
          ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(1000));
          for(ConsumerRecord record:records){
              logger.info("key: "+record.key()+" "+"value: "+record.value());
              logger.info("Partition: "+record.partition());
              logger.info("Offset: "+record.offset());
          }
        }

    }
}
