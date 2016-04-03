package com.github.hronom.test.spark.application.pools;

import com.google.common.io.Resources;

import com.github.hronom.test.spark.application.managers.ElasticsearchManager;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class ElasticsearchManagerPool {
    private static ElasticsearchManager elasticsearchManager = null;

    public static ElasticsearchManager getManager() throws IOException {
        if (elasticsearchManager == null) {
            elasticsearchManager = new ElasticsearchManager();
            elasticsearchManager.initialize("http://localhost:9200", 10);
            if (!elasticsearchManager.isIndexExists("test_index")) {
                URL url = ElasticsearchManagerPool.class.getResource("index_mapping.json");
                String indexConfigJson = Resources.toString(url, StandardCharsets.UTF_8);
                elasticsearchManager.addIndex("test_index", indexConfigJson);
            }
            else {
                elasticsearchManager.removeIndex("test_index");
            }
            return elasticsearchManager;
        } else {
            return elasticsearchManager;
        }
    }
}
