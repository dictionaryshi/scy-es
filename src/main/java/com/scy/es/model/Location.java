package com.scy.es.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author : shichunyang
 * Date    : 2022/12/1
 * Time    : 11:10 下午
 * ---------------------------------------
 * Desc    : Location
 */
@Getter
@Setter
@ToString
public class Location {

    /**
     * 经度
     */
    private Double lon;

    /**
     * 纬度
     */
    private Double lat;
}
