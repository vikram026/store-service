package com.nisum.impl;


import com.genericAvro.OfferLookupData;
import com.nisum.common.KafkaConstants;
import com.nisum.model.OfferData;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.ForeachWriter;

import java.util.Properties;

/*
 *   Actual flattening logic implementation and logic to push message to Division kafka by
 *   store ID
 * */
public class ForEachWriterImpl extends ForeachWriter<OfferData> {

    private static final long serialVersionUID = 1L;
    private Properties properties = new Properties();
    private Producer<String, OfferLookupData> producer;
    private String topic;
    private OfferLookupData offerLookupData;
    private ProducerRecord<String, OfferLookupData> producerRecord;

    @Override
    public boolean open(long l, long l1) {
        properties.setProperty("bootstrap.servers", KafkaConstants.KAFKA_SERVER);
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "1");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", KafkaConstants.SCHEMA_REGISTRY_URL);
        return true;
    }

    @Override
    public void process(OfferData offer) {
        try {
            producer = new KafkaProducer<String, OfferLookupData>(properties);
            topic = offer.getStoreId().trim().replaceAll(" ", "");
            offerLookupData = mapOffer(offer);
            producerRecord = new ProducerRecord<String, OfferLookupData>(
                    topic, offerLookupData
            );
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("metadata : " + metadata);
                    } else {
                        exception.printStackTrace();
                    }
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private OfferLookupData mapOffer(OfferData offer) {
        return OfferLookupData.newBuilder()
                .setOfferId(offer.getOfferId())
                .setOfferType(offer.getOfferType())
                .setId(offer.getId())
                .setPreCondition(offer.getPreCondition())
                .setStoreId(offer.getStoreId())
                .setTerminal(offer.getTerminal()).build();
    }

    @Override
    public void close(Throwable throwable) {

        System.out.println("Offer Processed ");
    }
}