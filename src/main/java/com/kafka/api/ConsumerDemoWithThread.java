package com.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    private ConsumerDemoWithThread() {

    }

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        // create the consumer runnable
        logger.info("Creating the consumer thread");
        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, "first_topic");
        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("the Application has exited");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is Closing ");
        }
    }

    public class ConsumerRunnable implements Runnable {
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private KafkaConsumer<String, String> consumer;
        private CountDownLatch latch;

        public ConsumerRunnable(CountDownLatch latch, String topic) {

            this.latch = latch;

            //1-create consumer config
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-six-application");
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //2-create consumer
            consumer = new KafkaConsumer<String, String>(props);
            //3-Subscribe consumer to our topic
            consumer.subscribe(Collections.singleton(topic));


        }


        @Override
        public void run() {
            //4- poll for new data
            try {
                while (true) {
                    //ConsumerRecords<String,String>records=consumer.poll(100);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key :" + record.key() + ", Value : " + record.value());
                        logger.info("Partition :" + record.partition() + ", Offset : " + record.offset());
                    }

                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutDown() {
            consumer.wakeup();
        }

    }
}
