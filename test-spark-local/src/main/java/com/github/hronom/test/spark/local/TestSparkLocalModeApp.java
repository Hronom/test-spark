package com.github.hronom.test.spark.local;

import com.github.hronom.test.spark.local.functions.ResultsDumperFunction;
import com.github.hronom.test.spark.local.functions.SpaceSplitFlatMapFunction;
import com.github.hronom.test.spark.local.receivers.JavaCustomReceiver;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class TestSparkLocalModeApp {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Spark test").setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
        JavaDStream<String> customReceiverStream = ssc.receiverStream(new JavaCustomReceiver());
        JavaDStream<String> words = customReceiverStream.flatMap(new SpaceSplitFlatMapFunction());
        words.foreachRDD(new ResultsDumperFunction());
        ssc.start();
        ssc.awaitTermination();
    }
}
