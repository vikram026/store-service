package com.nisum.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SparkSessionConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSessionConfig.class);

    public static SparkSession sparkSession() {
        LOGGER.info("Into Setting the configuartion of Spark ");
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("OfferReader")
                .set("spark.sql.warehouse.dir", "/tmp/SparkReadSession");
        return SparkSession.builder().config(sparkConf).getOrCreate();
    }
}