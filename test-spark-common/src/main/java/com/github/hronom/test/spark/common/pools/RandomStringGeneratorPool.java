package com.github.hronom.test.spark.common.pools;

import net.moznion.random.string.RandomStringGenerator;

public final class RandomStringGeneratorPool {
    private static RandomStringGenerator generator = null;

    private RandomStringGeneratorPool() {
    }

    public static RandomStringGenerator getGenerator() {
        if (generator == null) {
            generator = new RandomStringGenerator();
            return generator;
        } else {
            return generator;
        }
    }
}
