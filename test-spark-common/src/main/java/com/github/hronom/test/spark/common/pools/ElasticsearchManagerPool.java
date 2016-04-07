package com.github.hronom.test.spark.common.pools;

import com.google.common.io.Resources;

import com.github.hronom.test.spark.common.managers.ElasticsearchManager;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public final class ElasticsearchManagerPool {
    private static ElasticsearchManager elasticsearchManager = null;

    private ElasticsearchManagerPool() {
    }

    public static ElasticsearchManager getManager() throws IOException {
        if (elasticsearchManager == null) {
            ElasticsearchManager newElasticsearchManager = new ElasticsearchManager();
            newElasticsearchManager.initialize("http://localhost:9200", 10);
            if (!newElasticsearchManager.isIndexExists("test_index")) {
                URL url = ElasticsearchManagerPool.class.getResource("index_mapping.json");
                String indexConfigJson = Resources.toString(url, StandardCharsets.UTF_8);
                newElasticsearchManager.addIndex("test_index", indexConfigJson);
            }
            else {
                newElasticsearchManager.removeIndex("test_index");
            }
            elasticsearchManager = newElasticsearchManager;
            return elasticsearchManager;
        } else {
            return elasticsearchManager;
        }
    }
}
