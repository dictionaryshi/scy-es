package com.scy.es.config;

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

    /*@Bean(destroyMethod = "close")
    public RestHighLevelClient restHighLevelClient() {
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    @Bean
    public EsClient esClient(RestHighLevelClient restHighLevelClient) {
        return new EsClient(restHighLevelClient);
    }*/
}
