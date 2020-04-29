package com.kafka.api.search;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer_1 {

    //elasticSearch client
    public static RestHighLevelClient createClient() {

        String hostName = "kafka-api-608156863.ap-southeast-2.bonsaisearch.net";
        String userName = "uahkxa2bhi";
        String passWord = "thya8pr435";

        // don't do if you run a local elastic search
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, passWord));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;


    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        //1-create consumer config
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo4-elasticsearch");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //2-create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //3-Subscribe consumer to our topic
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer_1.class.getName());
        RestHighLevelClient client = createClient();

        //"twitter_tweets"
        KafkaConsumer<String, String> consumer = createConsumer("first_topic");

        // poll for new data

        while (true) {

            //ConsumerRecords<String,String>records=consumer.poll(100);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {

                // where we insert data to ElasticSearch
                String recordKey = record.key();
                String recordValue = record.value();
                String jsonString = null;
                if (recordKey != null && recordValue != null) {
                    jsonString = "{ \"+" + recordKey + "\" :\"+" + recordValue + "\" }";

                    // logger.info("key :"+ record.key() +", Value : "+record.value());
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);
                    try {
                        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                        String id = indexResponse.getId();
                        logger.info("id--->" + id);

                    } catch (IOException e) {
                        logger.error("Can not create elastic search :" + e);
                    }

                    try {
                        Thread.sleep(1000);// introduce a small delay
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

    }
}
// to test elastic search cloud
//1-https://app.bonsai.io/clusters/kafka-api-608156863/console
//2- /twitter/tweets/5SpOw3EBpGZ1RJF22dDg