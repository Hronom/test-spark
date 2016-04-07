package com.github.hronom.test.spark.common.functions;

import org.apache.spark.api.java.function.Function;

public class SimpleMapFunction implements Function<String, String> {
    @Override
    public String call(String str) {
        return "Processed string: " + str;
    }
}