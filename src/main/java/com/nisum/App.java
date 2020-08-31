package com.nisum;


import com.nisum.jobs.OfferReader;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws StreamingQueryException, RestClientException, IOException {

        log.info("Into Main method ");

        OfferReader offerReadJob = new OfferReader();
        offerReadJob.read();
    }
}
