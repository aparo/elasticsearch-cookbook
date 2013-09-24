package com.packtpub;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

/**
 * Created by alberto on 9/21/13.
 */
public class QueryHelper {
    private final Client client;
    private final IndicesOperations io;
    public QueryHelper() {
        this.client = NativeClient.createTransportClient();
        io = new IndicesOperations(client);
    }


    public void populateData(String index, String type) {
        if (io.checkIndexExists(index))
            io.deleteIndex(index);

        try {
            client.admin().indices().prepareCreate(index)
                    .addMapping(type, XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject(type)
                            .startObject("_timestamp").field("enabled", true).field("store", "yes").endObject()
                            .startObject("_ttl").field("enabled", true).field("store", "yes").endObject()
                            .startObject("properties")
                            .startObject("name")
                            .field("type", "string")
                            .field("term_vector", "with_positions_offsets")
                            .field("store", "yes")
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject())
                    .execute().actionGet();
        } catch (IOException e) {
            System.out.println("Unable to create mapping");
        }

        BulkRequestBuilder bulker = client.prepareBulk();
        for (Integer i = 1; i <= 1000; i++) {
            bulker.add(client.prepareIndex(index, type, i.toString()).setSource("text", i.toString(), "number1", i + 1, "number2", i % 2));
        }
        bulker.execute().actionGet();

        client.admin().indices().prepareRefresh(index).execute().actionGet();

    }

    public void dropIndex(String index){
        io.deleteIndex(index);
    }

    public Client getClient() {
        return client;
    }
}
