package com.nisum.impl;

import com.genericAvro.OfferLookupData;
import com.nisum.common.KafkaConstants;
import com.nisum.model.OfferData;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.*;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

public class ForEachWriterImplTest {

    @InjectMocks
    private ForEachWriterImpl forEachWriterImpl;

    @Mock
    private Producer<String, OfferLookupData> producer;

    @Spy
    private Properties properties = new Properties();

    private static Stream<Arguments> mockOfferDataSource() {
        return Stream.of(
                Arguments.of(mockOfferData())
        );
    }

    private static Stream<Arguments> openTest() {
        return Stream.of(
                Arguments.of(1, 1)
        );
    }

    private static OfferData mockOfferData() {
        OfferData offerData = new OfferData();
        offerData.setId("11111");
        offerData.setOfferId(1);
        offerData.setOfferType("someType");
        offerData.setStoreId("someStore");
        offerData.setPreCondition("someCondition");
        offerData.setTerminal("someTerminal");
        return offerData;
    }

    @BeforeEach
    public void setUp() {
        producer = new MockProducer<>();
        properties.setProperty("bootstrap.servers", KafkaConstants.KAFKA_SERVER);
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "1");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", KafkaConstants.SCHEMA_REGISTRY_URL);
        MockitoAnnotations.initMocks(this);
    }

    @DisplayName("This test will be test the  open method ")
    @ParameterizedTest
    @MethodSource
    public void openTest(long l, long l1) {

        boolean result = forEachWriterImpl.open(l, l1);
        assertEquals(result, true, () -> "open method should return true");

    }

    @DisplayName("This test will be test the  process method which was creating the sotres in divistion kafka")
    @ParameterizedTest
    @MethodSource("mockOfferDataSource")
    public void processTest(OfferData offerData) {
        when(producer.send(ArgumentMatchers.any(ProducerRecord.class), ArgumentMatchers.any(Callback.class))).thenReturn(Mockito.mock(Future.class));
        forEachWriterImpl.process(offerData);
    }

}
