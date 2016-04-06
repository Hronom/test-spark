package com.github.hronom.test.spark.application.functions;

import org.apache.spark.api.java.function.Function;

public class StringMapFunction implements Function<String, String> {
    @Override
    public String call(String s) {
        return "Processed string: " + s;
    }
}