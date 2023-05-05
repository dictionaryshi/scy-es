package com.scy.es;

import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.get.GetResult;
import co.elastic.clients.elasticsearch.core.mget.MultiGetOperation;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.core.msearch.MultiSearchItem;
import co.elastic.clients.elasticsearch.core.msearch.MultiSearchResponseItem;
import co.elastic.clients.elasticsearch.core.msearch.RequestItem;
import co.elastic.clients.elasticsearch.core.search.*;
import co.elastic.clients.util.NamedValue;
import co.elastic.clients.util.ObjectBuilder;
import com.scy.core.CollectionUtil;
import com.scy.es.model.User;

import java.util.*;

import com.scy.es.model.Location;
import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.*;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.scy.core.format.DateUtil;
import com.scy.core.format.MessageUtil;
import com.scy.es.model.Shop;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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
        propertyMap.put("avgPrice", Property.of(propertyBuilder -> propertyBuilder.long_(LongNumberProperty.of(builder -> builder))));
        propertyMap.put("cityId", Property.of(propertyBuilder -> propertyBuilder.integer(IntegerNumberProperty.of(builder -> builder))));
        propertyMap.put("shopId", Property.of(propertyBuilder -> propertyBuilder.integer(IntegerNumberProperty.of(builder -> builder))));
        propertyMap.put("shopName", Property.of(propertyBuilder -> propertyBuilder.keyword(KeywordProperty.of(builder -> builder.fields("completion", f -> f.completion(c -> c.analyzer("ik_smart")))))));
        propertyMap.put("shopPoi", Property.of(propertyBuilder -> propertyBuilder.geoPoint(GeoPointProperty.of(builder -> builder))));
        propertyMap.put("operateName", Property.of(propertyBuilder -> propertyBuilder.wildcard(builder -> builder)));
        propertyMap.put("createdAt", Property.of(propertyBuilder -> propertyBuilder.date(builder -> builder
                .format(DateUtil.PATTERN_SECOND)
        )));

        Map<String, Property> userPropertyMap = new HashMap<>(16);
        userPropertyMap.put("userName", Property.of(propertyBuilder -> propertyBuilder.keyword(KeywordProperty.of(builder -> builder.ignoreAbove(32).normalizer("lowercase_normalizer")))));
        userPropertyMap.put("age", Property.of(propertyBuilder -> propertyBuilder.integer(IntegerNumberProperty.of(builder -> builder.docValues(Boolean.FALSE)))));
        propertyMap.put("users", Property.of(propertyBuilder -> propertyBuilder.nested(builder -> builder
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
                .aliases("scy", a -> a)
        );

        try {
            CreateIndexResponse createIndexResponse = elasticsearchClient.indices().create(createIndexRequest);
            return createIndexResponse.acknowledged() && createIndexResponse.shardsAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    /*
        http://localhost:9200/_aliases
        {
          "actions": [
            {
              "remove": {
                "index": "shop",
                "alias": "scy"
              }
            },
            {
              "add": {
                "index": "new_shop",
                "alias": "scy"
              }
            }
          ]
        }
     */
    public ReindexResponse reIndex() {
        try {
            ReindexResponse reindexResponse = elasticsearchClient.reindex(r -> r
                    // 如果出现冲突, 如何处理。proceed:如果冲突, 忽略当前, 继续执行其他的。abort:如果冲突, 直接抛出异常, 中止后续的执行。
                    .conflicts(Conflicts.Proceed)
                    .source(s -> s
                            .index("shop")
                            .query(q -> q.matchAll(m -> m))
                            .size(2000)
                    )
                    .dest(d -> d
                            .index("new_shop")
                            .opType(OpType.Create)
                    )
                    .slices(Slices.of(s -> s.computed(SlicesCalculation.Auto)))
                    .requestsPerSecond(2000L)
                    .script(s -> s
                            .inline(i -> i
                                    .source("ctx._routing = ctx._source.shopId")
                            )
                    )
            );

            return reindexResponse;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
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

    public IndexResponse index() {
        Shop shop = new Shop();
        shop.setAddress("胜辛路426号2层222-3号商铺");
        shop.setAvgPrice(56_00L);
        shop.setCityId(1);
        shop.setShopId(10000);
        shop.setShopName("肯德基");
        shop.setShopPoi(new Location(121.25092315673828, 31.331241607666016));
        shop.setOperateName("史春阳");
        shop.setCreatedAt(new Date());
        shop.setUsers(CollectionUtil.newArrayList(new User("婷婷", 27)));

        IndexRequest<Shop> request = IndexRequest.of(i -> i
                .index("shop")
                .id("1001")
                .document(shop)
                .routing("a")
                .opType(OpType.Create)
                .refresh(Refresh.WaitFor)
                .timeout(builder -> builder
                        .time("200ms")
                )
        );

        try {
            return elasticsearchClient.index(request);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Shop get(String index, String id) {
        GetRequest request = GetRequest.of(i -> i
                .index(index)
                .id(id)
                .routing("a")
        );
        try {
            GetResponse<Shop> response = elasticsearchClient.get(request, Shop.class);
            if (response.found()) {
                Optional.ofNullable(response.source()).ifPresent(shop -> shop.setId(response.id()));
                return response.source();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public DeleteResponse delete(String index, String id) {
        try {
            DeleteRequest request = DeleteRequest.of(i -> i
                    .index(index)
                    .id(id)
                    .routing("a")
                    .refresh(Refresh.WaitFor)
                    .timeout(builder -> builder
                            .time("200ms")
                    )
            );
            return elasticsearchClient.delete(request);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public UpdateResponse<Shop> update(String index, String id) {
        Shop shop = new Shop();
        shop.setCityId(2);

        try {
            UpdateRequest<Shop, Shop> request = UpdateRequest.of(i -> i
                    .index(index)
                    .id(id)
                    .routing("a")
                    .refresh(Refresh.WaitFor)
                    .timeout(builder -> builder
                            .time("200ms")
                    )
                    .doc(shop)
                    .docAsUpsert(Boolean.FALSE)
                    .retryOnConflict(1)
                    .source(builder -> builder
                            .fetch(Boolean.TRUE)
                    )
            );
            return elasticsearchClient.update(request, Shop.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public UpdateByQueryResponse updateByQuery(String index) {
        try {
            UpdateByQueryRequest request = UpdateByQueryRequest.of(i -> i
                    .index(index)
                    .routing("a")
                    .refresh(Boolean.TRUE)
                    .scroll(builder -> builder
                            .time("60s"))
                    .scrollSize(2000L)
                    // 设置任务切分的片数, 可以理解为设置并行任务来进行查询, 默认为1, 这个值不建议设置超过10个, 一旦非常大, 会造成CPU飙高。
                    .slices(builder -> builder
                            .value(5)
                    )
                    // 每秒执行请求数
                    .requestsPerSecond(1000L)
                    // 如果出现冲突, 如何处理。proceed:如果冲突, 忽略当前, 继续执行其他的。abort:如果冲突, 直接抛出异常, 中止后续的执行。
                    .conflicts(Conflicts.Proceed)
                    // 忽略不可用索引
                    .ignoreUnavailable(Boolean.TRUE)
                    // 每个搜索请求的超时
                    .searchTimeout(builder -> builder
                            .time("1s")
                    )
                    .query(queryBuilder -> queryBuilder
                            .term(builder -> builder
                                    .field("cityId")
                                    .value(1L)
                            )
                    )
                    .script(scriptBuilder -> scriptBuilder
                            .inline(builder -> builder
                                    .source("if (ctx._source.avgPrice == 5600) {ctx._source.avgPrice = 9600;}")
                            )
                    )
            );
            return elasticsearchClient.updateByQuery(request);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public DeleteByQueryResponse deleteByQuery(String index) {
        try {
            DeleteByQueryRequest request = DeleteByQueryRequest.of(i -> i
                    .index(index)
                    .routing("a")
                    .refresh(Boolean.TRUE)
                    .scroll(builder -> builder
                            .time("60s"))
                    .scrollSize(2000L)
                    // 设置任务切分的片数, 可以理解为设置并行任务来进行查询, 默认为1, 这个值不建议设置超过10个, 一旦非常大, 会造成CPU飙高。
                    .slices(builder -> builder
                            .value(5)
                    )
                    // 每秒执行请求数
                    .requestsPerSecond(1000L)
                    // 如果出现冲突, 如何处理。proceed:如果冲突, 忽略当前, 继续执行其他的。abort:如果冲突, 直接抛出异常, 中止后续的执行。
                    .conflicts(Conflicts.Proceed)
                    // 忽略不可用索引
                    .ignoreUnavailable(Boolean.TRUE)
                    // 每个搜索请求的超时
                    .searchTimeout(builder -> builder
                            .time("1s")
                    )
                    .query(queryBuilder -> queryBuilder
                            .term(builder -> builder
                                    .field("cityId")
                                    .value(1L)
                            )
                    )
            );
            return elasticsearchClient.deleteByQuery(request);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public BulkResponse bulk(List<Shop> shops) {
        BulkRequest.Builder builder = new BulkRequest.Builder();
        builder.refresh(Refresh.WaitFor);
        builder.timeout(t -> t
                .time("1s"));

        for (Shop shop : shops) {
            builder.operations(op -> op
                    .create(i -> i
                            .index("shop")
                            .id(shop.getId())
                            .document(shop)
                            .routing("a")
                    )

            );
        }

        try {
            BulkResponse response = elasticsearchClient.bulk(builder.build());
            if (response.errors()) {
                for (BulkResponseItem item : response.items()) {
                    if (item.error() != null) {
                        System.out.println(item.error().reason());
                    }
                }
            }
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public SearchResponse<Shop> search(String index, Function<Query.Builder, ObjectBuilder<Query>> queryBuilder, List<SortOptions> sortOptions) {
        try {
            SearchResponse<Shop> response = elasticsearchClient.search(s -> s
                            .index("shop")
                            .routing("a")
                            .timeout("500ms")
                            .allowPartialSearchResults(Boolean.TRUE)
                            .allowNoIndices(Boolean.TRUE)
                            .ignoreUnavailable(Boolean.TRUE)
                            .searchType(SearchType.QueryThenFetch)
                            .maxConcurrentShardRequests(5L)
                            .from(0)
                            .size(30)
                            .query(queryBuilder)
                            .sort(sortOptions)
//                            .highlight(h -> h.fields("address", HighlightField.of(hi -> hi.highlightQuery(queryBuilder))))
                    , Shop.class
            );

            TotalHits total = response.hits().total();
            boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

            if (isExactResult) {
                System.out.println("There are " + total.value() + " results");
            } else {
                System.out.println("There are more than " + total.value() + " results");
            }

            List<Hit<Shop>> hits = response.hits().hits();
            for (Hit<Shop> hit : hits) {
                Shop shop = hit.source();
                shop.setId(hit.id());
                System.out.println("Found product " + shop + ", score " + hit.score());
            }

            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void scroll(String index) {
        try {
            SearchResponse<Shop> response = elasticsearchClient.search(s -> s
                            .index(index)
                            .routing("a")
                            .timeout("500ms")
                            .allowPartialSearchResults(Boolean.TRUE)
                            .ignoreUnavailable(Boolean.TRUE)
                            .searchType(SearchType.QueryThenFetch)
                            .maxConcurrentShardRequests(5L)
                            .from(0)
                            .size(1)
                            .query(q -> q.matchAll(m -> m))
                            .scroll(sc -> sc.time("60s"))
                    , Shop.class
            );

            TotalHits total = response.hits().total();
            boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

            if (isExactResult) {
                System.out.println("There are " + total.value() + " results");
            } else {
                System.out.println("There are more than " + total.value() + " results");
            }

            List<Hit<Shop>> hits = response.hits().hits();
            for (Hit<Shop> hit : hits) {
                Shop shop = hit.source();
                shop.setId(hit.id());
                System.out.println("Found product " + shop + ", score " + hit.score());
            }

            String scrollId = response.scrollId();
            ScrollResponse<Shop> scroll = null;
            do {
                scroll = elasticsearchClient.scroll(s -> s
                                .scrollId(scrollId)
                                .scroll(sc -> sc.time("60s"))
                        , Shop.class);

                hits = scroll.hits().hits();
                for (Hit<Shop> hit : hits) {
                    Shop shop = hit.source();
                    shop.setId(hit.id());
                    System.out.println("Found product " + shop + ", score " + hit.score());
                }
            } while (hits.size() > 0);

            ClearScrollResponse clearScrollResponse = elasticsearchClient.clearScroll(c -> c.scrollId(CollectionUtil.newArrayList(scrollId)));
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void mget(String index) {
        try {
            MgetResponse<Shop> response = elasticsearchClient.mget(mg -> mg
                            .docs(CollectionUtil.newArrayList(MultiGetOperation.of(m -> m.index("shop").routing("a").id("1001")),
                                    MultiGetOperation.of(m -> m.index("shop").routing("a").id("1002"))))
                    , Shop.class);

            List<MultiGetResponseItem<Shop>> docs = response.docs();
            for (MultiGetResponseItem<Shop> doc : docs) {
                if (doc.isResult()) {
                    GetResult<Shop> result = doc.result();
                    if (result.found()) {
                        Shop shop = result.source();
                        shop.setId(result.id());
                        System.out.println("Found product " + shop);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void msearch(String index) {
        try {
            MsearchResponse<Shop> response = elasticsearchClient.msearch(ms -> ms
                            .maxConcurrentShardRequests(5L)
                            .searches(CollectionUtil.newArrayList(RequestItem.of(r -> r.body(b -> b
                                    .timeout("500ms")
                                    .from(0)
                                    .size(30)
                                    .query(q -> q.matchAll(m -> m))
                            ).header(h -> h
                                    .index(index)
                                    .routing("a")
                                    .allowPartialSearchResults(Boolean.TRUE)
                                    .allowNoIndices(Boolean.TRUE)
                                    .ignoreUnavailable(Boolean.TRUE)
                                    .searchType(SearchType.QueryThenFetch)
                            )), RequestItem.of(r -> r.body(b -> b
                                    .timeout("500ms")
                                    .from(0)
                                    .size(30)
                                    .query(q -> q.matchAll(m -> m))
                            ).header(h -> h
                                    .index(index)
                                    .routing("a")
                                    .allowPartialSearchResults(Boolean.TRUE)
                                    .allowNoIndices(Boolean.TRUE)
                                    .ignoreUnavailable(Boolean.TRUE)
                                    .searchType(SearchType.QueryThenFetch)
                            ))))
                    , Shop.class);

            long took = response.took();
            List<MultiSearchResponseItem<Shop>> responses = response.responses();
            for (MultiSearchResponseItem<Shop> multiSearchResponseItem : responses) {
                if (multiSearchResponseItem.isResult()) {
                    MultiSearchItem<Shop> result = multiSearchResponseItem.result();

                    TotalHits total = result.hits().total();
                    boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

                    if (isExactResult) {
                        System.out.println("There are " + total.value() + " results");
                    } else {
                        System.out.println("There are more than " + total.value() + " results");
                    }

                    took = result.took();

                    List<Hit<Shop>> hits = result.hits().hits();
                    for (Hit<Shop> hit : hits) {
                        Shop shop = hit.source();
                        shop.setId(hit.id());
                        System.out.println("Found product " + shop + ", score " + hit.score());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public SearchResponse<Shop> suggest(String index) {
        try {
            SearchResponse<Shop> response = elasticsearchClient.search(s -> s
                            .index(index)
                            .routing("a")
                            .timeout("500ms")
                            .allowPartialSearchResults(Boolean.TRUE)
                            .ignoreUnavailable(Boolean.TRUE)
                            .searchType(SearchType.QueryThenFetch)
                            .maxConcurrentShardRequests(5L)
                            .suggest(sug -> sug
                                    .suggesters("shopNameSug", su -> su
                                            .completion(c -> c.field("shopName.completion").size(10).skipDuplicates(Boolean.TRUE))
                                            .text("笔记本")
                                    ))
                    , Shop.class
            );

            Map<String, List<Suggestion<Shop>>> suggest = response.suggest();
            Suggestion<Shop> shopNameSug = suggest.get("shopNameSug").get(0);
            CompletionSuggest<Shop> completion = shopNameSug.completion();
            List<CompletionSuggestOption<Shop>> options = completion.options();
            for (CompletionSuggestOption<Shop> option : options) {
                String text = option.text();
                System.out.println(text);
            }
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public SearchResponse<Shop> metric() {
        try {
            SearchResponse<Shop> response = elasticsearchClient.search(s -> s
                            .index("shop")
                            .routing("a")
                            .timeout("500ms")
                            .allowPartialSearchResults(Boolean.TRUE)
                            .allowNoIndices(Boolean.TRUE)
                            .ignoreUnavailable(Boolean.TRUE)
                            .searchType(SearchType.QueryThenFetch)
                            .maxConcurrentShardRequests(5L)
                            .from(0)
                            .size(30)
                            .query(q -> q.matchAll(m -> m))
                            .aggregations("avg", a -> a.avg(av -> av.field("avgPrice")))
                            .aggregations("sum", a -> a.sum(su -> su.field("avgPrice")))
                            .aggregations("min", a -> a.min(mi -> mi.field("avgPrice")))
                            .aggregations("max", a -> a.max(ma -> ma.field("avgPrice")))
                            .aggregations("cardinality", a -> a.cardinality(ca -> ca.field("avgPrice")))
                            .aggregations("stats", a -> a.stats((st -> st.field("avgPrice"))))

                            .aggregations("terms", a -> a.terms(t -> t.field("avgPrice").order(NamedValue.of("_count", SortOrder.Desc)).size(3))
                                    .aggregations("cityId", ag -> ag.sum(su -> su.field("cityId"))))
                    , Shop.class
            );

            TotalHits total = response.hits().total();
            boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

            if (isExactResult) {
                System.out.println("There are " + total.value() + " results");
            } else {
                System.out.println("There are more than " + total.value() + " results");
            }

            List<Hit<Shop>> hits = response.hits().hits();
            for (Hit<Shop> hit : hits) {
                Shop shop = hit.source();
                shop.setId(hit.id());
                System.out.println("Found product " + shop + ", score " + hit.score());
            }

            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
