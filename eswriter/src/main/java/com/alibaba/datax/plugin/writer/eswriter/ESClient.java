package com.alibaba.datax.plugin.writer.eswriter;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;


/**
 * Created by Rassyan on 18/7/16.
 */
public class ESClient {
    private RestHighLevelClient client;

    public RestHighLevelClient getClient() {
        return client;
    }

    public void createClient(List<String> hosts) {
        HttpHost[] httpHosts = new HttpHost[hosts.size()];
        for (int i = 0; i < hosts.size(); i++) {
            httpHosts[i] = HttpHost.create(hosts.get(i));
        }
        client = new RestHighLevelClient(RestClient.builder(httpHosts));
    }

    public void closeClient() throws Exception {
        if (client != null) {
            client.close();
        }
    }
}
