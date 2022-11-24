package com.scy.es.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.scy.core.json.JsonUtil;
import com.scy.es.EsClient;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Bean;

/**
 * EsConfig
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/25.
 */
public class EsConfig {

    @Bean(destroyMethod = "close")
    public ElasticsearchTransport elasticsearchTransport() {
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200)).build();

        return new RestClientTransport(restClient, new JacksonJsonpMapper(JsonUtil.getBaseObjectMapper()));
    }

    @Bean
    public EsClient esClient(ElasticsearchTransport elasticsearchTransport) {
        return new EsClient(new ElasticsearchClient(elasticsearchTransport));
    }
}
