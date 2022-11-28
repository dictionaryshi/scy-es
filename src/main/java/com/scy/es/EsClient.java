package com.scy.es;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.*;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.scy.core.format.DateUtil;
import com.scy.core.format.MessageUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * EsClient
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/25.
 */
@Getter
@Slf4j
public class EsClient {

    private final ElasticsearchClient elasticsearchClient;

    private final ElasticsearchAsyncClient elasticsearchAsyncClient;

    public EsClient(ElasticsearchClient elasticsearchClient, ElasticsearchAsyncClient elasticsearchAsyncClient) {
        this.elasticsearchClient = elasticsearchClient;
        this.elasticsearchAsyncClient = elasticsearchAsyncClient;
    }

    public boolean ping() {
        try {
            return elasticsearchClient.ping().value();
        } catch (Exception e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    public void pingAsync() {
        CompletableFuture<BooleanResponse> responseCompletableFuture = elasticsearchAsyncClient.ping();
        responseCompletableFuture.whenComplete((response, exception) -> {
            if (Objects.nonNull(exception)) {
                System.out.println(MessageUtil.format("es ping error", exception));
            } else {
                System.out.println(MessageUtil.format("es ping", "result", response.value()));
            }
        });
    }

    public boolean createIndex() {
        Map<String, Property> propertyMap = new HashMap<>(16);
        propertyMap.put("address", Property.of(propertyBuilder -> propertyBuilder.text(TextProperty.of(builder -> builder
                // ik_max_word
                .analyzer("ik_smart")
                .searchAnalyzer("ik_smart")
        ))));
        propertyMap.put("avgprice", Property.of(propertyBuilder -> propertyBuilder.long_(LongNumberProperty.of(builder -> builder))));
        propertyMap.put("cityid", Property.of(propertyBuilder -> propertyBuilder.integer(IntegerNumberProperty.of(builder -> builder))));
        propertyMap.put("shopid", Property.of(propertyBuilder -> propertyBuilder.integer(IntegerNumberProperty.of(builder -> builder))));
        propertyMap.put("shopname", Property.of(propertyBuilder -> propertyBuilder.keyword(KeywordProperty.of(builder -> builder))));
        propertyMap.put("shoppoi", Property.of(propertyBuilder -> propertyBuilder.geoPoint(GeoPointProperty.of(builder -> builder))));
        propertyMap.put("operateName", Property.of(propertyBuilder -> propertyBuilder.wildcard(builder -> builder)));
        propertyMap.put("createdAt", Property.of(propertyBuilder -> propertyBuilder.date(builder -> builder
                .format(DateUtil.PATTERN_SECOND)
        )));

        Map<String, Property> userPropertyMap = new HashMap<>(16);
        userPropertyMap.put("userName", Property.of(propertyBuilder -> propertyBuilder.keyword(KeywordProperty.of(builder -> builder.ignoreAbove(32).normalizer("lowercase_normalizer")))));
        userPropertyMap.put("age", Property.of(propertyBuilder -> propertyBuilder.integer(IntegerNumberProperty.of(builder -> builder.docValues(Boolean.FALSE)))));
        propertyMap.put("user", Property.of(propertyBuilder -> propertyBuilder.nested(builder -> builder
                .properties(userPropertyMap)
        )));

        CreateIndexRequest createIndexRequest = CreateIndexRequest.of(createIndexBuilder -> createIndexBuilder
                .index("shop")
                .includeTypeName(Boolean.FALSE)
                .mappings(typeMappingBuilder -> typeMappingBuilder
                        .dynamic(DynamicMapping.False)
                        .routing(builder -> builder.required(Boolean.FALSE))
                        .properties(propertyMap)
                )
                .settings(indexSettingsBuilder -> indexSettingsBuilder // .routingPartitionSize(5) (routing必须为true)
                        .numberOfShards("5")
                        .numberOfReplicas("1")
                        .codec("best_compression")
                        .analysis(analysisBuilder -> analysisBuilder
                                .normalizer("lowercase_normalizer", normalizerBuilder -> normalizerBuilder
                                        .custom(builder -> builder.filter("lowercase"))
                                )
                        )
                )
        );

        try {
            CreateIndexResponse createIndexResponse = elasticsearchClient.indices().create(createIndexRequest);
            return createIndexResponse.acknowledged() && createIndexResponse.shardsAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    public boolean putSettings() {
        try {
            PutIndicesSettingsResponse putIndicesSettingsResponse = elasticsearchClient.indices().putSettings(settingsBuilder -> settingsBuilder
                    .index("shop")
                    .settings(builder -> builder
                            .numberOfReplicas("3")
                    )
            );
            return putIndicesSettingsResponse.acknowledged();
        } catch (Exception e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    public boolean deleteIndex(String index) {
        try {
            DeleteIndexResponse deleteIndexResponse = elasticsearchClient.indices().delete(deleteBuilder -> deleteBuilder
                    .index(index)
            );
            return deleteIndexResponse.acknowledged();
        } catch (Exception e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    public boolean putMapping() {
        Map<String, Property> propertyMap = new HashMap<>(16);

        PutMappingRequest putMappingRequest = PutMappingRequest.of(putMappingBuilder -> putMappingBuilder
                .index("shop")
                .dynamic(DynamicMapping.False)
                .includeTypeName(Boolean.FALSE)
                .routing(builder -> builder.required(Boolean.FALSE))
                .properties(propertyMap)
        );
        try {
            PutMappingResponse putMappingResponse = elasticsearchClient.indices().putMapping(putMappingRequest);
            return putMappingResponse.acknowledged();
        } catch (Exception e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    public String getMapping(String index) {
        try {
            return elasticsearchClient.indices().getMapping(getMappingBuilder -> getMappingBuilder.index(index)).toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getSettings(String index) {
        try {
            return elasticsearchClient.indices().getSettings(getSettingsBuilder -> getSettingsBuilder.index(index)).toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /*
     *//**
     * 索引文档, 文档存在则替换
     *//*
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

    *//**
     * 根据id查询文档
     *//*
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

    *//**
     * 根据id删除文档
     *//*
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

    *//**
     * 更新文档
     *//*
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

    *//**
     * 搜索
     *//*
    public SearchBO search(SearchAO searchAO) {
        SearchRequest searchRequest = new SearchRequest(searchAO.getIndexes().toArray(ArrayUtil.EMPTY_STRING_ARRAY));
        // 忽略不存在的索引
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(searchAO.getQueryBuilder());
        searchSourceBuilder.from(searchAO.getFrom());
        searchSourceBuilder.size(searchAO.getSize());
        searchSourceBuilder.timeout(new TimeValue(searchAO.getTimeoutSeconds(), TimeUnit.SECONDS));

        if (!CollectionUtil.isEmpty(searchAO.getSortBuilders())) {
            searchAO.getSortBuilders().forEach(searchSourceBuilder::sort);
        }

        if (!ObjectUtil.isNull(searchAO.getAggregationBuilder())) {
            searchSourceBuilder.aggregation(searchAO.getAggregationBuilder());
        }
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            log.info(MessageUtil.format("ES search response", "indexes", searchAO.getIndexes(), "queryBuilder", searchAO.getQueryBuilder(), "searchAO", searchAO, "searchResponse", searchResponse));
            return convertSearchResponse(searchResponse);
        } catch (Exception e) {
            log.error(MessageUtil.format("ES search error", e, "indexes", searchAO.getIndexes(), "queryBuilder", searchAO.getQueryBuilder(), "searchAO", searchAO));
            return null;
        }
    }

    private SearchBO convertSearchResponse(SearchResponse searchResponse) {
        SearchHits hits = searchResponse.getHits();

        SearchBO searchBO = new SearchBO();
        searchBO.setTotal(hits.getTotalHits().value);

        SearchHit[] searchHits = hits.getHits();
        List<String> documents = Stream.of(searchHits).map(SearchHit::getSourceAsString).collect(Collectors.toList());
        searchBO.setDocuments(documents);

        searchBO.setAggregations(searchResponse.getAggregations());
        return searchBO;
    }*/
}
