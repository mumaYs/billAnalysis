package com.ccfsoft.bigdata.billAnalysis.spark.entity;

import java.io.Serializable;

public class TestEntity implements Serializable {
    private String location;
    private String price;
    private String title;

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
