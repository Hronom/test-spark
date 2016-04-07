package com.github.hronom.test.spark.common.functions;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;

public class SplitBySpaceFlatMapFunction implements FlatMapFunction<String, String> {
    // TODO Function Not serializable exception
    //private final RandomStringGenerator randomStringGenerator = new RandomStringGenerator();

    @Override
    public Iterable<String> call(String line) throws Exception {
        //randomStringGenerator.generateByRegex("");
        return Arrays.asList(line.split(" "));
    }
}