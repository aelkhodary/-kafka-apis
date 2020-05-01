package com.kafka.api.stream;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    // we will go and fitter some tweets

    public static void main(String[] args) {
        //create properties
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                // fitter for tweets which has a user of over 1000 followers
                (k, jsonTweet) -> extractUserFollowersTweet(jsonTweet) > 1000
        );
        filteredStream.to("important_tweets");

        // build the topology

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);


        // start our streams applications

        kafkaStreams.start();
    }

    static JsonParser jsonParser = new JsonParser();

    private static Integer extractUserFollowersTweet(String tweetJson) {
        // gson library
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (Exception e) {
            return 0;
        }

    }
}
