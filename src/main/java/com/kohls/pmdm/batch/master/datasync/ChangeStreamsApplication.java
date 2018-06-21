package com.kohls.pmdm.batch.master.datasync;

import com.kohls.pmdm.batch.master.common.Constants;
import com.kohls.pmdm.batch.master.common.MongoClientHolder;
import com.mongodb.BasicDBObject;
import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.session.ClientSession;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class ChangeStreamsApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamsApplication.class);

    private boolean changeStreamEnable = true;
    private String collectionList = "t0,t1,t2,t3";
    private String collectionSuffix = "_coll";

    //@Autowired
    MongoClientHolder mongoClientHolder = new MongoClientHolder();
    //@Autowired
    //MongoDatabase mongoDatabase;
    //@Autowired
    MongoClient client = mongoClientHolder.getClient();


    private   ChangeStreamService changeStreamService = new ChangeStreamService();



    public static void main(String[] args) { SpringApplication.run(ChangeStreamsApplication.class, args);}

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {

        LOGGER.info("Starting ChangeStreamAppRunner run method");

        changeStreamService.setClient(client);
        changeStreamService.setMongoDatabase(  mongoClientHolder.mongoDatabase());

        if (changeStreamEnable) {
            MongoDatabase database = mongoClientHolder.mongoDatabase();
            List<String> collections = new ArrayList<>();
            MongoIterable<String> dbCollectionList = database.listCollectionNames();
            List<String> collectionNames = Arrays.asList(collectionList.split(","));
            for (String collectionValue : dbCollectionList) {
                collections.add(collectionValue);
            }
            // Create Sequence collection to keep the change stream tokens for the above
            // collections.
            if (!collections.contains(Constants.SEQUENCE)) {
                database.createCollection(Constants.SEQUENCE);
            }
            MongoCollection<Document> collection = database.getCollection(Constants.SEQUENCE);
            LOGGER.info("Checking the change streams of collections {}", collectionNames);
            collectionNames.forEach(collectionName -> {
                String newCollection = collectionName + collectionSuffix;
                ClientSessionOptions sessionOptions = ClientSessionOptions.builder().build();
                ClientSession session = client.startSession(sessionOptions);
                LOGGER.info("Session casually consistent value: {} ", session.isCausallyConsistent());
                if (!collections.contains(newCollection)) {
                    database.createCollection(newCollection);
                    LOGGER.info("Created a new Collection : " + newCollection);
                }
                // Check whether the Id documents are present for the above collections else
                // create it.
                BasicDBObject whereQuery = new BasicDBObject();
                whereQuery.put(Constants.ID, collectionName + Constants.TOKEN);
                FindIterable<Document> sequenceIter = collection.find(whereQuery);
                Document document = sequenceIter.first();
                if (document == null) {
                    collection.insertOne(new Document(Constants.ID, collectionName + Constants.TOKEN));
                }

                changeStreamService.pollChangeStream(collectionName, session);
            });
        } else {
            LOGGER.info("Change Stream operations are not enabled");
        }
        LOGGER.info("Ending ChangeStreamAppRunner run method");

        return args -> {
            System.out.println("returning...");

        };
    }

}
