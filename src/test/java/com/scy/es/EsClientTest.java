package com.scy.es;

import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoValidationMethod;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.ZeroTermsQuery;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.google.common.collect.Lists;
import com.scy.core.CollectionUtil;
import com.scy.core.GeoUtil;
import com.scy.core.format.DateUtil;
import com.scy.core.json.JsonUtil;
import com.scy.core.thread.ThreadUtil;
import com.scy.es.config.EsConfig;
import com.scy.es.model.Shop;
import org.gavaghan.geodesy.GlobalCoordinates;
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
                , Lists.newArrayList());
        System.out.println();
    }

    @Test
    public void termsQueryTest() {
        SearchResponse<Shop> response = esClient.search("shop", builder -> builder
                .terms(t -> t
                        .field("cityId")
                        .terms(b -> b.value(Lists.newArrayList(FieldValue.of(1L), FieldValue.of(2L))))
                ), Lists.newArrayList());
        System.out.println();
    }

    @Test
    public void geoDistanceQueryTest() {
        double distance = GeoUtil.getDistance(new GlobalCoordinates(31.358, 121.25092315673828), new GlobalCoordinates(31.331241607666016, 121.25092315673828));

        SearchResponse<Shop> response = esClient.search("shop", builder -> builder
                        .bool(boolQueryBuilder -> boolQueryBuilder
                                .must(a -> a.term(t -> t.field("shopName").value("肯德基")))
                                .must(b -> b.geoDistance(g -> g
                                        .field("shopPoi")
                                        .validationMethod(GeoValidationMethod.Strict)
                                        .location(ge -> ge.latlon(l -> l.lon(121.25092315673828).lat(31.358)))
                                        .distance("2976m")
                                        .distanceType(GeoDistanceType.Arc)
                                ))
                        )
                , Lists.newArrayList(SortOptions.of(s -> s.geoDistance(g -> g
                        .location(ge -> ge.latlon(l -> l.lon(121.25092315673828).lat(31.358)))
                        .distanceType(GeoDistanceType.Arc)
                        .field("shopPoi")
                        .order(SortOrder.Asc)
                        .mode(SortMode.Min)
                        .unit(DistanceUnit.Meters)
                        .ignoreUnmapped(Boolean.FALSE)
                ))));
        System.out.println();
    }

    @Test
    public void prefixQueryTest() {
        SearchResponse<Shop> response = esClient.search("shop", builder -> builder
                        .prefix(p -> p.field("shopName").value("肯德"))
                , Lists.newArrayList());
        System.out.println();
    }

    @Test
    public void fuzzyQueryTest() {
        SearchResponse<Shop> response = esClient.search("shop", builder -> builder
                        .fuzzy(f -> f.field("address").value("426写错").fuzziness("2").prefixLength(1).maxExpansions(1).transpositions(Boolean.FALSE))
                , Lists.newArrayList());
        System.out.println();
    }

    @Test
    public void wildcardQueryTest() {
        SearchResponse<Shop> response = esClient.search("shop", builder -> builder
                        .wildcard(w -> w.field("operateName").wildcard("史春*"))
                , Lists.newArrayList());
        System.out.println();
    }

    @Test
    public void regexQueryTest() {
        SearchResponse<Shop> response = esClient.search("shop", builder -> builder
                        .regexp(r -> r.field("shopName").value("肯.+"))
                , Lists.newArrayList());
        System.out.println();
    }

    @Test
    public void rangeQueryTest() {
        SearchResponse<Shop> response = esClient.search("shop", builder -> builder
                        .range(r -> r.field("createdAt")
                                .format(DateUtil.PATTERN_SECOND)
                                .gte(JsonData.of("2022-12-23 19:18:18")).lte(JsonData.of("2022-12-23 19:18:18")))
                , Lists.newArrayList());
        System.out.println();
    }

    @Test
    public void boolQueryTest() {
        SearchResponse<Shop> response = esClient.search("shop", builder -> builder
                        .bool(b -> b
                                .should(s1 -> s1.bool(b1 -> b1
                                        .filter(f -> f.term(t -> t.field("shopId").value(10000)))
                                        .must(m -> m.term(t -> t.field("cityId").value(1)))
                                        .mustNot(mn -> mn.term(t -> t.field("shopName").value("华莱士")))
                                ))
                                .should(s2 -> s2.bool(b2 -> b2
                                        .must(m -> m.range(r -> r.field("createdAt")
                                                .format(DateUtil.PATTERN_SECOND)
                                                .gte(JsonData.of("2022-12-28 11:24:04")).lte(JsonData.of("2022-12-28 11:24:04"))))
                                ))
                        )
                , Lists.newArrayList());
        System.out.println();
    }

    @Test
    public void matchQueryTest() {
        SearchResponse<Shop> response = esClient.search("shop", builder -> builder
                        .match(m -> m
                                .field("address")
                                .query("426写错")
                                .fuzziness("AUTO")
                                .prefixLength(1)
                                .maxExpansions(1)
                                .fuzzyTranspositions(Boolean.FALSE)
                                .operator(Operator.Or)
                                .autoGenerateSynonymsPhraseQuery(Boolean.TRUE)
                                .lenient(Boolean.FALSE)
                                .zeroTermsQuery(ZeroTermsQuery.All)
                        )
                , Lists.newArrayList());
        System.out.println();
    }
}
