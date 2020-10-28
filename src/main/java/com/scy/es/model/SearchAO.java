package com.scy.es.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.util.List;

/**
 * SearchAO
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/28.
 */
@Getter
@Setter
@ToString
public class SearchAO {

    private List<String> indexes;

    private QueryBuilder queryBuilder;

    private int from;

    private int size;

    private int timeoutSeconds;

    private List<SortBuilder<?>> sortBuilders;

    private AggregationBuilder aggregationBuilder;
}
