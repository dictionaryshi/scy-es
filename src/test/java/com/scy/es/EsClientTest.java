package com.scy.es;

import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.scy.core.thread.ThreadUtil;
import com.scy.es.config.EsConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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
}
