package com.github.hronom.test.spark.application;

import com.github.hronom.test.spark.common.functions.ResultsToElasticsearchFunction;
import com.github.hronom.test.spark.common.functions.ResultsToElasticsearchWithCollectFunction;
import com.github.hronom.test.spark.common.functions.SimpleMapFunction;
import com.github.hronom.test.spark.common.receivers.JavaCustomWithCountReceiver;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;

public class TestSparkApplicationApp {
    public static void main(String[] args) {
        String propFileName = "config.properties";
        try (InputStream inputStream = Files.newInputStream(Paths.get(propFileName))) {
            Properties prop = new Properties();
            prop.load(inputStream);

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
                .setAppName(prop.getProperty("appName"))
                .setMaster(prop.getProperty("master"))
                .setJars(listOfJars.toArray(new String[listOfJars.size()]));
            JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
            JavaDStream<String> customReceiverStream = ssc.receiverStream(new JavaCustomWithCountReceiver(1000));
            //JavaDStream<String> transformedStrings = customReceiverStream.flatMap(new SpaceSplitFlatMapFunction());
            JavaDStream<String> transformedStrings = customReceiverStream.map(new SimpleMapFunction());
            transformedStrings.foreachRDD(new ResultsToElasticsearchFunction());
            ssc.start();
            ssc.awaitTermination();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
