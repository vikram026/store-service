package com.nisum.jobs;

import com.nisum.common.KafkaConstants;
import com.nisum.config.SparkSessionConfig;
import com.nisum.impl.ForEachWriterImpl;
import com.nisum.model.OfferData;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

import static org.apache.spark.sql.Encoders.bean;

/*
 *   Spark job to consume and have flattening logic
 * */
public class OfferReader {

    private static final SparkSession sparkSession = SparkSessionConfig.sparkSession();

    private static final SQLContext sqlContext = SparkSessionConfig.sparkSession().sqlContext();

    public void read() throws IOException, RestClientException, StreamingQueryException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(KafkaConstants.SCHEMA_REGISTRY_URL, 128);
        SchemaMetadata metadata = cachedSchemaRegistryClient.getLatestSchemaMetadata(KafkaConstants.TOPIC_NAME + "-value");
        Schema schema = cachedSchemaRegistryClient.getById(metadata.getId());
        StructType dataType = (StructType) SchemaConverters.toSqlType(schema).dataType();

        sparkSession.udf().register("deserialize", (byte[] data) -> {
            CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(KafkaConstants.SCHEMA_REGISTRY_URL, 128);
            KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
            GenericRecord record = (GenericRecord) kafkaAvroDeserializer.deserialize("", data);
            return RowFactory.create(Long.parseLong("" + record.get("offerId")), record.get("offerType").toString(),
                    record.get("storeId").toString(), record.get("terminal").toString(),
                    record.get("id").toString(), record.get("preCondition").toString()
            );
        }, dataType);

        Dataset<OfferData> dataFrame = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KafkaConstants.KAFKA_SERVER)
                .option("subscribe", KafkaConstants.TOPIC_NAME)
                .option("startingOffsets", KafkaConstants.START_OFFSET)
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("deserialize(value) as value")
                .select("value.*")
                .as(bean(OfferData.class));
        dataFrame.createOrReplaceTempView("offerMetaDataView");
        Dataset<OfferData> employeeDataSet = sqlContext.sql("select * from offerMetaDataView").as(bean(OfferData.class));
        employeeDataSet.writeStream().foreach(new ForEachWriterImpl()).outputMode(OutputMode.Append()).option("checkpointLocation", "/tmp/OfferLookupKafkaCheckpointagain").start().awaitTermination();
    }

}
