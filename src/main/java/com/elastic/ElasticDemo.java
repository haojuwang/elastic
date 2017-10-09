package com.elastic;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lihao on 17/10/9.
 */
public class ElasticDemo {
    public static void main(String... arg) throws Exception {

        Settings settings = Settings.builder()
                .put("cluster.name", "lw-6-test").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.10.6"), 9300));

        insert(client);

        client.close();
    }


    /**
     * 获取
     * @param client
     * @throws Exception
     */
    public static void prepareGet(TransportClient client) throws Exception {
        GetResponse response = client.prepareGet("mytest", "test", "p1").get();
        System.out.println(response);
    }


    /**
     * insert
     * @param client
     * @throws Exception
     */
    public static void insert(TransportClient client) throws Exception {
        Map<String,Object> resource = new HashMap<>();
        resource.put("name","lenow note");
        resource.put("price",8877);
        resource.put("description","mac Note 新款");
        IndexRequestBuilder index = client.prepareIndex("mytest", "test");

        IndexResponse insertResponse = index.setSource(resource).execute().get();

        System.out.println(insertResponse);
    }


    /**
     * 删除
     * @param client
     * @throws Exception
     */
    public static void delete(TransportClient client) throws Exception{
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("name", "mac"))
                .source("mytest")
                .get();


       long deleted = response.getDeleted();
       System.out.println("删除个数: "+deleted);

    }


    public static void update(TransportClient client) throws Exception{

        UpdateRequest queryRequest =  new UpdateRequest();
        queryRequest.index("mytest");
        queryRequest.type("test");




    }
}
