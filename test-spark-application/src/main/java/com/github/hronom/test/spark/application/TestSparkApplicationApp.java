package com.github.hronom.test.spark.application;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class TestSparkApplicationApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
            .setAppName("Spark test")
            .setMaster("spark://NOMAD-MAIN:7077");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
        ssc.start();
        ssc.awaitTermination();
    }
}
