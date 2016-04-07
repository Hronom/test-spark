package com.github.hronom.test.spark.common.pools;

import net.moznion.random.string.RandomStringGenerator;

public class RandomStringGeneratorPool {
    private static RandomStringGenerator generator = null;

    public static RandomStringGenerator getGenerator() {
        if (generator == null) {
            generator = new RandomStringGenerator();
            return generator;
        } else {
            return generator;
        }
    }
}
