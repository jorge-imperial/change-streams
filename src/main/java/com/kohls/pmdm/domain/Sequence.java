package com.kohls.pmdm.domain;

public class Sequence {

    public String id;
    public String bsonToken;

    public void set_id(String id) {
        this.id = id;
    }

    public void setBsonToken(String bsonToken) {
         this.bsonToken = bsonToken;
    }

    public String getBsonToken() {
        return this.bsonToken;
    }
}
