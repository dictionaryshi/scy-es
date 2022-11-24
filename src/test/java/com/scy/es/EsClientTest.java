package com.scy.es;

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
}
