package com.kafka.api;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {



    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        //1-create producer properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //2- create the producer

        KafkaProducer<String ,String > producer= new KafkaProducer<String, String>(props);
        for (int i=0;i<10;i++) {
            //create producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world"+Integer.toString(i));

            //3-send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //the record successfully send
                        logger.info("Received new metadata \n" + "Topic:" + recordMetadata.topic() + "\n"
                                + "partition :" + recordMetadata.partition() + "\n"
                                + "Offset :" + recordMetadata.offset() + "\n"
                                + "Timestamp :" + recordMetadata.timestamp());
                    } else {

                        logger.error("Error while producing " + e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();


        logger.info("hello world");

    }
}
