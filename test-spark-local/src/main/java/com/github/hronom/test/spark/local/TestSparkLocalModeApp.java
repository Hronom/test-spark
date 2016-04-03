package com.github.hronom.test.spark.local;

import com.github.hronom.test.spark.local.functions.ResultsDumperFunction;
import com.github.hronom.test.spark.local.functions.SpaceSplitFlatMapFunction;
import com.github.hronom.test.spark.local.receivers.JavaCustomReceiver;

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

public class TestSparkLocalModeApp {
    public static void main(String[] args) throws InterruptedException {
        ArrayList<String> listOfJars = new ArrayList<>();
        listOfJars.add("test-spark-local-1.0.0.jar");
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
            .setMaster("local[2]")
            .setJars(listOfJars.toArray(new String[listOfJars.size()]));
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
        JavaDStream<String> customReceiverStream = ssc.receiverStream(new JavaCustomReceiver());
        JavaDStream<String> words = customReceiverStream.flatMap(new SpaceSplitFlatMapFunction());
        words.foreachRDD(new ResultsDumperFunction());
        ssc.start();
        ssc.awaitTermination();
    }
}
