package com.scy.es;

import com.scy.core.StringUtil;
import com.scy.core.format.MessageUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

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

    /**
     * 索引文档, 文档存在则替换
     */
    public String index(String index, String id, String json) {
        IndexRequest request = new IndexRequest(index);
        request.id(id);
        request.source(json, XContentType.JSON);
        request.timeout(TimeValue.timeValueSeconds(5));
        // 立即刷新
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        // 文档存在则替换
        request.opType(DocWriteRequest.OpType.INDEX);

        try {
            IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);
            log.info(MessageUtil.format("ES index response", "index", index, "id", id, "json", json, "indexResponse", indexResponse));
            return indexResponse.getId();
        } catch (Exception e) {
            log.error(MessageUtil.format("ES index error", e, "index", index, "id", id, "json", json));
            return StringUtil.EMPTY;
        }
    }

    /**
     * 根据id查询文档
     */
    public String get(String index, String id) {
        GetRequest getRequest = new GetRequest(index, id);
        try {
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            log.info(MessageUtil.format("ES get response", "index", index, "id", id, "getResponse", getResponse));
            return getResponse.getSourceAsString();
        } catch (Exception e) {
            log.error(MessageUtil.format("ES get error", e, "index", index, "id", id));
            return StringUtil.EMPTY;
        }
    }
}
