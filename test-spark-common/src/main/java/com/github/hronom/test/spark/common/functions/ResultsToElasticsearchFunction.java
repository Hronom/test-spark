package com.github.hronom.test.spark.common.functions;

import com.github.hronom.test.spark.common.managers.ElasticsearchManager;
import com.github.hronom.test.spark.common.pools.ElasticsearchManagerPool;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;

public class ResultsToElasticsearchFunction implements VoidFunction<JavaRDD<String>> {
    @Override
    public void call(JavaRDD<String> rdd) throws Exception {
        rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> stringIterator) throws Exception {
                ElasticsearchManager manager = ElasticsearchManagerPool.getManager();
                while (stringIterator.hasNext()) {
                    String str = stringIterator.next();
                    String json = "{\"testString\":\"" + StringEscapeUtils.escapeJson(str) + "\"}";
                    manager.putDocument("test_index", "result", json);
                }
                //Thread.sleep(TimeUnit.MINUTES.toMillis(1));
            }
        });
    }
}