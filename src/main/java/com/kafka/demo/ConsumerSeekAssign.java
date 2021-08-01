package com.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerSeekAssign {
    public static void main(String[] args) {
        String bootstrapServers="127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(ConsumerSeekAssign.class.getName());
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); // Latest if want to read from latest message , earliest if want to read from the very beginning
        // create consumer
        KafkaConsumer<String,String> consumer =  new KafkaConsumer<String, String>(properties);

        //assign and seek are used to mostly replay and fetch a specific message

        //assign
        TopicPartition partitionToreadFrom=new TopicPartition("myTopic",0);
        long offsetToReadFrom=10L;
        consumer.assign(Arrays.asList(partitionToreadFrom));

        //seek
        consumer.seek(partitionToreadFrom,offsetToReadFrom);

        int numberOfMessagesToRead=5;
        boolean keepOnReading=true;
        int numberOfMessagesReadSoFar=0;

        // poll for new data
        while(keepOnReading){
          ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(1000));
          for(ConsumerRecord record:records){
              logger.info("key: "+record.key()+" "+"value: "+record.value());
              logger.info("Partition: "+record.partition());
              logger.info("Offset: "+record.offset());
              if(numberOfMessagesReadSoFar>numberOfMessagesToRead)
              {
                  keepOnReading=false;
                  break;
              }
          }
        }

    }
}
