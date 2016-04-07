package com.github.hronom.test.spark.common.functions;

import com.github.hronom.test.spark.common.managers.ElasticsearchManager;
import com.github.hronom.test.spark.common.pools.ElasticsearchManagerPool;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;
import java.util.List;

public class ResultsToElasticsearchWithCollectFunction implements VoidFunction<JavaRDD<String>> {
    @Override
    public void call(JavaRDD<String> rdd) throws Exception {
        ElasticsearchManager manager = ElasticsearchManagerPool.getManager();
        List<String> list = rdd.collect();
        System.out.println("Collect happens... current thread ID: " + Thread.currentThread().getId());
        Iterator<String> stringIterator = list.iterator();
        while (stringIterator.hasNext()) {
            String str = stringIterator.next();

            String json = "{\"testString\":\"" + StringEscapeUtils.escapeJson(str) + "\"}";
            manager.putDocument("test_index", "result", json);
        }
    }
}