package com.scy.es.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.elasticsearch.search.aggregations.Aggregations;

import java.util.List;

/**
 * SearchBO
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/28.
 */
@Getter
@Setter
@ToString
public class SearchBO {

    private long total;

    private List<String> documents;

    private Aggregations aggregations;
}
