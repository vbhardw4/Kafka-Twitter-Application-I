package com.github.kafka.consumers;

import com.google.gson.JsonParser;
import com.twitter.hbc.core.Constants;
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
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.action.bulk.BulkRequest;
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
import java.util.Arrays;
import java.util.Properties;

public class ElasticSeachConsumer {

    public static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static String GROUP_ID = "twitter-application";
    public static String TOPIC = "twitter-tweets";

    public static RestHighLevelClient createClient(){

        String hostname = "kafkatwitterapplicat-1084675763.us-east-1.bonsaisearch.net"; // localhost or bonsai url
        String username = "ibfmymtg58"; // needed only for bonsai
        String password = "9i6vfw91ga"; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                        new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSeachConsumer.class.getName());
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        while(true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            logger.info("Received "+records.count()+" number of records");
            String recordId = null;
            for(ConsumerRecord<String,String> record: records) {
                String jsonString = record.value();
                try {
                    recordId = extractIdFromRecord(record.value());
                }
                catch(Exception e) {
                    logger.warn("Skipping the bad data... "+record.value());
                }

                IndexRequest indexRequest = new IndexRequest("twitter","tweets",recordId)
                        .source(jsonString,XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info("Id is "+indexResponse.getId());
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("committing the offsets");
            consumer.commitSync();
            logger.info("Offsets have been committed");
            try {
                Thread.sleep(5000);
            }
            catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
    }

    private static String extractIdFromRecord(String tweet) {
        return new JsonParser().parse(tweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static KafkaConsumer<String,String> createKafkaConsumer() {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // Disable auto-committing of offsets
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        consumerProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");



        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(consumerProperties);
        consumer.subscribe(Arrays.asList(TOPIC));
        return consumer;

    }

}
