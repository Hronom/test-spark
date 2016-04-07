package com.github.hronom.test.spark.common.managers;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.Health;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;

public class ElasticsearchManager {
    private URL serverUrl;
    private JestClient jestClient;

    public boolean initialize(String serverUrlArg, int connTimeout) {
        try {
            serverUrl = new URL(serverUrlArg);
        } catch (MalformedURLException exception) {
            System.err.println("Fail! " + exception.toString());
            return false;
        }
        if (!createJestClient(serverUrlArg, connTimeout)) {
            return false;
        }
        if (!checkClusterAvailability()) {
            return false;
        }
        return true;
    }

    public void close() {
        if (jestClient != null) {
            jestClient.shutdownClient();
        }
    }

    public boolean checkClusterAvailability() {
        try {
            Health health = new Health.Builder().build();
            JestResult requestResult = jestClient.execute(health);
            if (!requestResult.isSucceeded()) {
                System.err
                    .println("Unable to check cluster health: " + requestResult.getErrorMessage());
                return false;
            }
        } catch (Exception exception) {
            System.err.println("Fail! " + exception.toString());
            return false;
        }
        return true;
    }

    public boolean addIndex(String indexName, String indexConfigJson) {
        try {
            CreateIndex createIndex =
                new CreateIndex
                    .Builder(indexName)
                    .settings(indexConfigJson)
                    .build();
            JestResult requestResult = jestClient.execute(createIndex);
            if (!requestResult.isSucceeded()) {
                throw
                    new Exception("Unable to create index: " + requestResult.getErrorMessage());
            }
        } catch (Exception exception) {
            System.err.println("Fail! " + exception.toString());
            return false;
        }
        return true;
    }

    public boolean isIndexExists(String indexName)
        throws IOException {
        IndicesExists indicesExists = new IndicesExists.Builder(indexName).build();
        return jestClient.execute(indicesExists).isSucceeded();
    }

    public boolean removeIndex(String indexName) {
        try {
            DeleteIndex deleteIndex = new DeleteIndex.Builder(indexName).build();
            JestResult requestResult = jestClient.execute(deleteIndex);
            if (!requestResult.isSucceeded()) {
                throw
                    new Exception("Unable to delete index: " + requestResult.getErrorMessage());
            }
        } catch (Exception exception) {
            System.err.println("Fail! " + exception.toString());
            return false;
        }
        return true;
    }

    public String putDocument(String indexName, String typeName, String json) {
        try {
            Index.Builder builder = new Index.Builder(json);
            builder.index(indexName);
            builder.type(typeName);
            Index index = builder.build();
            JestResult requestResult = jestClient.execute(index);
            if (!requestResult.isSucceeded()) {
                throw new Exception(
                    "Unable to put document to Elasticsearch: " + requestResult.getErrorMessage());
            }
            return (String) requestResult.getValue("_id");
        } catch (Exception exception) {
            System.err.println("Fail! " + exception.toString());
            return null;
        }
    }

    private boolean createJestClient(String serverURL, int connTimeout) {
        if (serverURL == null || serverURL.trim().length() == 0) {
            System.err.println("Parameter serverURL is missing.");
            return false;
        }
        HttpClientConfig clientConfig =
            new HttpClientConfig
                .Builder(serverURL)
                .multiThreaded(true)
                .connTimeout(connTimeout)
                .build();
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(clientConfig);
        jestClient = factory.getObject();
        return true;
    }
}
