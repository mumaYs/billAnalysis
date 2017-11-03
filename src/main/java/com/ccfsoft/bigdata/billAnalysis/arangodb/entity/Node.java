package com.ccfsoft.bigdata.billAnalysis.arangodb.entity;

import com.arangodb.entity.DocumentField;
import com.arangodb.entity.DocumentField.Type;

public class Node {
    @DocumentField(Type.ID)
    private String id;

    @DocumentField(Type.KEY)
    private String key;

    @DocumentField(Type.REV)
    private String revision;

    private Long phone;

    public Node(String key,Long phone) {
        this.key = key;
        this.phone = phone;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getRevision() {
        return revision;
    }

    public void setRevision(String revision) {
        this.revision = revision;
    }

    public Long getPhone() {
        return phone;
    }

    public void setPhone(Long phone) {
        this.phone = phone;
    }
}