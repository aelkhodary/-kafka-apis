package com.kafka.api.search;

import com.google.gson.JsonParser;
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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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

public class ElasticSearchConsumer_4 {

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
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo12-elasticsearch");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");// disabled auto commit of offsets
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        //2-create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //3-Subscribe consumer to our topic
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer_4.class.getName());
        RestHighLevelClient client = createClient();

        //"twitter_tweets"
        KafkaConsumer<String, String> consumer = createConsumer("first_topic");

        // poll for new data

        while (true) {

            //ConsumerRecords<String,String>records=consumer.poll(100);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            logger.info("Received " + records.count() + " records");
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                //2 strategies to make id
                // first option kafka generic id
                String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                // second option twitter feed specific id
                // String id=extractIdFromTweet(record.value());

                // where we insert data to ElasticSearch
                String recordKey = record.key();
                String recordValue = record.value();
                String jsonString = null;
                if (recordKey != null && recordValue != null) {
                    jsonString = "{ \"+" + recordKey + "\" :\"+" + recordValue + "\" }";

                    // logger.info("key :"+ record.key() +", Value : "+record.value());
                   /* IndexRequest indexRequest = new IndexRequest("twitter", "tweets"
                            , id // this to make our consumer idempotent
                    ).source(jsonString, XContentType.JSON);*/
                    IndexRequest indexRequest = new IndexRequest("post");
                    indexRequest.index("twitter");
                    indexRequest.type("tweets");
                    indexRequest.id(id);
                    indexRequest.source(jsonString, XContentType.JSON);
                    bulkRequest.add(indexRequest); //we add to our bulk request(takes no time)

                }
            }
            try {
                BulkResponse bulkItemResponses=client.bulk(bulkRequest , RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("Commiting offsets....");
            consumer.commitSync();
            logger.info("Offsets have been commited....");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson) {
        // gson library
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();

    }
}
// to test elastic search cloud
//1-https://app.bonsai.io/clusters/kafka-api-608156863/console
//2- /twitter/tweets/5SpOw3EBpGZ1RJF22dDg