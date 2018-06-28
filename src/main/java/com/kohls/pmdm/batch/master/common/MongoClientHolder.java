package com.kohls.pmdm.batch.master.common;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;


import org.springframework.stereotype.Component;

@Component
public class MongoClientHolder {

    private static MongoClient client;
    private static MongoDatabase db;

    public MongoClientHolder() {
        client = new MongoClient();
        db = client.getDatabase("test");
    }

    public MongoDatabase mongoDatabase() {
        return db;
    }

    public MongoClient getClient() {
        return client;
    }
}
