package com.github.hronom.test.spark.application;

import com.github.hronom.test.spark.application.functions.ResultsDumperFunction;
import com.github.hronom.test.spark.application.functions.SpaceSplitFlatMapFunction;
import com.github.hronom.test.spark.application.receivers.JavaCustomReceiver;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class TestSparkApplicationApp {
    public static void main(String[] args) {
        ArrayList<String> listOfJars = new ArrayList<>();
        listOfJars.add("test-spark-application-1.0.0.jar");
        try (DirectoryStream<Path> stream = Files
            .newDirectoryStream(Paths.get("lib"), "*.jar")) {
            for (Path path : stream) {
                if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) && !Files.isHidden(path)) {
                    listOfJars.add(path.toString());
                }
            }
        } catch (IOException exception) {
        }
        SparkConf conf = new SparkConf()
            .setAppName("Spark test")
            .setMaster("spark://localhost:7077")
            .setJars(listOfJars.toArray(new String[listOfJars.size()]));
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
        JavaDStream<String> customReceiverStream = ssc.receiverStream(new JavaCustomReceiver());
        JavaDStream<String> strings = customReceiverStream.flatMap(new SpaceSplitFlatMapFunction());
        strings.foreachRDD(new ResultsDumperFunction());
        ssc.start();
        ssc.awaitTermination();
    }
}
