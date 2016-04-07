package com.github.hronom.test.spark.common.functions;

import com.github.hronom.test.spark.common.managers.ElasticsearchManager;
import com.github.hronom.test.spark.common.pools.ElasticsearchManagerPool;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;

public class ResultsToElasticsearchWithCollectFunction implements VoidFunction<JavaRDD<String>> {
    @Override
    public void call(JavaRDD<String> rdd) throws Exception {
        ElasticsearchManager manager = ElasticsearchManagerPool.getManager();
        Iterator<String> stringIterator = rdd.collect().iterator();
        while (stringIterator.hasNext()) {
            String str = stringIterator.next();

            System.out.println(Thread.currentThread().getId());

            String json = "{\"testString\":\"" + StringEscapeUtils.escapeJson(str) + "\"}";
            manager.putDocument("test_index", "result", json);
        }
    }
}