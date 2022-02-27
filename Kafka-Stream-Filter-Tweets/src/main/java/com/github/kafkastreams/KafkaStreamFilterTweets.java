package com.github.kafkastreams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamFilterTweets {

    public static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static String TOPIC = "twitter-tweets";
    public static int MAX_FOLLOWERS_COUNT_TO_FILTER = 10000;

    public static void main(String[] args) {
        // Create Properties

        Properties properties = new Properties();

        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //Create Topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic

        KStream<String, String> inputTopic = streamsBuilder.stream(TOPIC);
        KStream<String, String> filteredStream = inputTopic.filter(
                (k,jsonTweet) -> extractUserFollowersInTweet(jsonTweet)
            );

        filteredStream.to("filtered-tweets");

        //Build the topology

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);
    }

    private static boolean extractUserFollowersInTweet(String jsonTweet) {
        try {
            return new JsonParser().parse(jsonTweet)
                    .getAsJsonObject()
                    .get("followers_count").getAsInt() > MAX_FOLLOWERS_COUNT_TO_FILTER;
        }
        catch (Exception e) {
            return false;
        }

    }
}
