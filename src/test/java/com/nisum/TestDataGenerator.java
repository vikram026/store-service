package com.nisum;

import com.genericAvro.OfferLookupData;
import com.nisum.common.KafkaConstants;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/*
*   Generator to generate Offer Data to offerLkpConnect topic
* */
public class TestDataGenerator {

    static Properties properties = new Properties();

    @Test
    public void prepareData() {
        generatorTestData();
    }

    /*
     *  Generate Offer Data to offerLkpConnect topic and returns the storeIDs from the offer data for further assertions
     * */
    public List<String> generatorTestData() {

        properties.setProperty("bootstrap.servers", KafkaConstants.KAFKA_SERVER);
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", KafkaConstants.SCHEMA_REGISTRY_URL);

        Producer<String, OfferLookupData> producer = new KafkaProducer<>(properties);

        return LookUpGenerator.generateLookUpData(10).stream().map(offerLookupData -> {
            produceMessageToTopic(producer, offerLookupData);
            return offerLookupData.getStoreId().trim().replaceAll(" ", "");
        }).collect(Collectors.toList());
    }

    private void produceMessageToTopic(Producer<String, OfferLookupData> producer, OfferLookupData offerLookupData) {
        ProducerRecord<String, OfferLookupData> producerRecord = new ProducerRecord<>(
                KafkaConstants.TOPIC_NAME, offerLookupData
        );
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                System.out.println(metadata);
            } else {
                exception.printStackTrace();
            }
        });
    }
}
