package com.scy.es;

import com.scy.core.StringUtil;
import com.scy.core.format.MessageUtil;
import com.scy.es.model.SearchAO;
import com.scy.es.model.SearchBO;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;

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

    /**
     * 根据id删除文档
     */
    public boolean delete(String index, String id) {
        DeleteRequest request = new DeleteRequest(index, id);
        request.timeout(TimeValue.timeValueSeconds(5));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
            log.info(MessageUtil.format("ES delete response", "index", index, "id", id, "deleteResponse", deleteResponse));
            return Boolean.TRUE;
        } catch (Exception e) {
            log.error(MessageUtil.format("ES delete error", e, "index", index, "id", id));
            return Boolean.FALSE;
        }
    }

    /**
     * 更新文档
     */
    public String update(String index, String id, String json) {
        UpdateRequest request = new UpdateRequest(index, id);
        request.doc(json, XContentType.JSON);
        request.timeout(TimeValue.timeValueSeconds(5));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        request.fetchSource(Boolean.TRUE);
        try {
            UpdateResponse updateResponse = restHighLevelClient.update(request, RequestOptions.DEFAULT);
            log.info(MessageUtil.format("ES update response", "index", index, "id", id, "json", json, "updateResponse", updateResponse));
            GetResult result = updateResponse.getGetResult();
            return result.sourceAsString();
        } catch (Exception e) {
            log.error(MessageUtil.format("ES update error", e, "index", index, "id", id, "json", json));
            return StringUtil.EMPTY;
        }
    }

    /**
     * 搜索
     */
    public SearchBO search(SearchAO searchAO) {
    }
}
