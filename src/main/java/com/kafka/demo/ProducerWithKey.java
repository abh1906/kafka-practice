package com.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKey {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithKey.class);
        //create producer properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());// to tell what type of value we are going to send
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        String key="Hello";
        //create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties); //<Key type,Value type>
        for(int i=0;i<10;i++) {

            key=key+i;
            logger.info("key: "+key);

            // create producer record

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("myTopic", key+i,"hello" + i);

            // send data -asynchronous

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Topic :" + recordMetadata.topic());
                        logger.info("Partition:" + recordMetadata.partition());
                        logger.info("offset:" + recordMetadata.offset());

                    } else {
                        logger.error(e.getMessage());
                    }
                }
            }); // .get() here if want to block send and use synchronous send
        }
        //flush data
        producer.flush();

        //flush and close producer
        producer.close();


    }
}
