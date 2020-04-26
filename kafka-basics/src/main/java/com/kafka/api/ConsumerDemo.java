package com.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        //1-create consumer config
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-fourth-application");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //2-create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(props);
        //3-Subscribe consumer to our topic
        consumer.subscribe(Collections.singleton("first_topic"));
        // to subscribe to more topics used Array
        // consumer.subscribe(Arrays.asList("first_topic","second_topic"));

        //4- poll for new data

        while(true){

            //ConsumerRecords<String,String>records=consumer.poll(100);
            ConsumerRecords<String,String> records =consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> record:records){

                logger.info("key :"+ record.key() +", Value : "+record.value());

                logger.info("Partition :"+ record.partition() +", Offset : "+record.offset());


            }

        }

    }
}
