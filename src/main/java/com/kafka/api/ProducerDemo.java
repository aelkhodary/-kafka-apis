package com.kafka.api;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {



    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        //1-create producer properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //2- create the producer

        KafkaProducer<String ,String > producer= new KafkaProducer<String, String>(props);
        //create producer record
        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic","hello world");

        //3-send data
        producer.send(record);

        producer.flush();
        producer.close();


        logger.info("hello world");

    }
}
