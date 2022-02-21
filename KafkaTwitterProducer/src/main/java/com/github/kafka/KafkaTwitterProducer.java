package com.github.kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaTwitterProducer {
    Logger logger = LoggerFactory.getLogger(KafkaTwitterProducer.class.getName());
    public static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static String CONSUMER_KEY = "jkNPgyFj5pp2QJkdKRWPuCfCk";
    public static String CONSUMER_SECRET = "cxjxlhy0c7mnLOKP8TarxOYcZkkf862Gv1GwWznuUq9nQuqGpN";
    //BEARER TOKEN "AAAAAAAAAAAAAAAAAAAAAPTUZQEAAAAA0UdexuRO4SOnaZ5jhxVfhcS%2F8Rk%3DCcY0wMUkFa1xti9XpNBetCO3tfTrHsPvYXd8yGgsEig8UL2opn";
    public static String TOKEN = "1953746335-ay3VOwepU65fdLarik4jjvBLlH85SgzuiE1oKy3";
    public static String SECRET = "OAL0yZqvJRxTMwpzd8HK49XxZNN1O719JBzbaw1UtvqEm";

    public static void main(String[] args) {
        new KafkaTwitterProducer().run();
    }

    public void run() {
        // Create the client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // Create the Kafka Producer and send the messages to the Kafka Topic

        Producer<String,String> kafkaProducer = createKafkaProducer();

        // Adding a runtime shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping the application");
            logger.info("Disconnecting the client connection");
            client.stop();
            logger.info("Closing the producer");
            kafkaProducer.close();
        }));


        // poll the messages matching with the search term
        while (!client.isDone()) {
            String retrievedMessage;
            try {
                retrievedMessage = msgQueue.poll(5, TimeUnit.SECONDS);
                logger.info("Message got is "+retrievedMessage);
                kafkaProducer.send(new ProducerRecord<>("twitter-tweets", retrievedMessage), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null) {
                            logger.error("something went wrong "+e);
                        }
                    }
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
        }
        logger.info("Application is stopped now..");
    }

    private Producer<String,String> createKafkaProducer() {
        Properties kafkaProducerProperties = new Properties();

        kafkaProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        kafkaProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);

        // SAFE PRODUCER
        // Make the producer idempotent
        kafkaProducerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        kafkaProducerProperties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        kafkaProducerProperties.setProperty(ProducerConfig.RETRIES_CONFIG,String.valueOf(Integer.MAX_VALUE));
        kafkaProducerProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        // High Throughput producer at the expense of latency and CPU usage
        kafkaProducerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"5");
        kafkaProducerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        kafkaProducerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,String.valueOf(32*1024)); //32KB


        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(kafkaProducerProperties);

        return producer;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
//        LOG.info("Creating the twitter client");
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("bitcoin","elections");
//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        return builder.build();
    }
}
