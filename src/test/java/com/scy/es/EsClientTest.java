package com.scy.es;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.InlineGet;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.google.common.collect.Lists;
import com.scy.core.CollectionUtil;
import com.scy.core.json.JsonUtil;
import com.scy.core.thread.ThreadUtil;
import com.scy.es.config.EsConfig;
import com.scy.es.model.Shop;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * @author : shichunyang
 * Date    : 2022/11/24
 * Time    : 5:27 下午
 * ---------------------------------------
 * Desc    : EsClientTest
 */
public class EsClientTest {

    private EsClient esClient;

    private final EsConfig esConfig = new EsConfig();

    private ElasticsearchTransport elasticsearchTransport;

    @Before
    public void before() {
        this.elasticsearchTransport = esConfig.elasticsearchTransport();
        this.esClient = esConfig.esClient(elasticsearchTransport);
    }

    @After
    public void after() {
        try {
            elasticsearchTransport.close();
            System.out.println("es客户端关闭");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void pingTest() {
        boolean ping = esClient.ping();
        Assert.assertTrue(ping);
    }

    @Test
    public void pingAsyncTest() {
        esClient.pingAsync();
        ThreadUtil.quietSleep(3_000);
    }

    @Test
    public void createIndexTest() {
        boolean rsult = esClient.createIndex();
        System.out.println();
    }

    @Test
    public void getMappingTest() {
        String mapping = esClient.getMapping("shop");
        System.out.println();
    }

    @Test
    public void getSettingsTest() {
        String settings = esClient.getSettings("shop");
        System.out.println();
    }

    @Test
    public void putMappingTest() {
        boolean result = esClient.putMapping();
        System.out.println();
    }

    @Test
    public void deleteIndexTest() {
        boolean result = esClient.deleteIndex("shop");
        System.out.println();
    }

    @Test
    public void putSettingsTest() {
        boolean result = esClient.putSettings();
        System.out.println();
    }

    @Test
    public void indexTest() {
        IndexResponse indexResponse = esClient.index();
        System.out.println();
    }

    @Test
    public void getTest() {
        Shop shop = esClient.get("shop", "1001");
        System.out.println(JsonUtil.object2Json(shop));
    }

    @Test
    public void deleteTest() {
        DeleteResponse deleteResponse = esClient.delete("shop", "1001");
        System.out.println();
    }

    @Test
    public void updateTest() {
        UpdateResponse<Shop> response = esClient.update("shop", "1001");
        Optional.ofNullable(response.get()).map(InlineGet::source).ifPresent(shop -> System.out.println(JsonUtil.object2Json(shop)));
    }

    @Test
    public void updateByQueryTest() {
        UpdateByQueryResponse response = esClient.updateByQuery("shop");
        System.out.println();
    }

    @Test
    public void deleteByQueryTest() {
        DeleteByQueryResponse response = esClient.deleteByQuery("shop");
        System.out.println();
    }

    @Test
    public void bulkTest() {
        List<Shop> shops = CollectionUtil.newArrayList();
        BulkResponse response = esClient.bulk(shops);
        System.out.println();
    }

    @Test
    public void termQueryTest() {
        SearchResponse<Shop> response = esClient.search("shop", builder -> builder
                .term(t -> t
                        .field("cityId")
                        .value(1L))
        );
        System.out.println();
    }

    @Test
    public void termsQueryTest() {
        SearchResponse<Shop> response = esClient.search("shop", builder -> builder
                .terms(t -> t
                        .field("cityId")
                        .terms(b -> b.value(Lists.newArrayList(FieldValue.of(1L), FieldValue.of(2L))))
                ));
        System.out.println();
    }
}
