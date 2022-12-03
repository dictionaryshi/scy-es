package com.scy.es.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

/**
 * @author : shichunyang
 * Date    : 2022/12/1
 * Time    : 11:14 下午
 * ---------------------------------------
 * Desc    : Shop
 */
@Getter
@Setter
@ToString
public class Shop {

    private String id;

    private String address;

    private Long avgPrice;

    private Integer cityId;

    private Integer shopId;

    private String shopName;

    private Location shopPoi;

    private String operateName;

    private Date createdAt;

    private User user;
}
