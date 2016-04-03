package com.github.hronom.test.spark.application.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class ResultsDumperFunction implements VoidFunction<JavaRDD<String>> {
    @Override
    public void call(JavaRDD<String> rdd) throws Exception {
        rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> stringIterator) throws Exception {
                while (stringIterator.hasNext()) {
                    System.out.println(stringIterator.next());
                }
                Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                System.out.println();
            }
        });
    }
}