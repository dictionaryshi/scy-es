package com.scy.es;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * EsClient
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/25.
 */
@Slf4j
public class EsClient {

    private final RestHighLevelClient restHighLevelClient;

    public EsClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }
}
