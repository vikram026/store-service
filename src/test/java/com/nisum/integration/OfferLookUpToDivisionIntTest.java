package com.nisum.integration;

import com.genericAvro.OfferLookupData;
import com.nisum.TestDataGenerator;
import com.nisum.jobs.OfferReader;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/*
*   Class to check integration testing
* */
public class OfferLookUpToDivisionIntTest {

    static List<String> topics = new ArrayList<>();
    private OfferReader offerReader = new OfferReader();

    @BeforeAll
    public static void setUp() {
        TestDataGenerator testDataGenerator = new TestDataGenerator();
        topics = testDataGenerator.generatorTestData();
    }

    @Test
    public void read() {
        new Thread(() -> {
            try {
                offerReader.read();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RestClientException e) {
                e.printStackTrace();
            } catch (StreamingQueryException e) {
                e.printStackTrace();
            }
        }).start();

        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    topics.forEach(topic -> {
                        readStoreTopic(topic);
                    });
                });
    }

    /*
    *   Method to assert the division kafka topics with store IDs from offer data
    * */
    private void readStoreTopic(String topic) {
        Consumer<Long, OfferLookupData> kafkaConsumer = new KafkaConsumer<>(getKafkaConsumerProperties());
        kafkaConsumer.subscribe(Collections.singleton(topic));
        AtomicInteger i = new AtomicInteger();
        AtomicReference<Boolean> runConfig = new AtomicReference<>(true);
        while (runConfig.get()) {
            i.getAndIncrement();
            if (i.get() == 10) {
                runConfig.set(false);
            }
            ConsumerRecords<Long, OfferLookupData> records = kafkaConsumer.poll(100);
            records.forEach(record -> {
                Assertions.assertTrue(record.value().getStoreId().trim().replaceAll(" ", "").equals(topic));
                runConfig.set(false);
            });
        }
    }


    public Properties getKafkaConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaConsumer" + Math.random());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "300");
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        return properties;
    }

}
