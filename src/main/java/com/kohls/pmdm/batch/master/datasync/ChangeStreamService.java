package com.kohls.pmdm.batch.master.datasync;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.gson.Gson;
import com.kohls.pmdm.batch.master.common.*;
import com.kohls.pmdm.domain.*;

import com.mongodb.BasicDBObject;
import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClient;
import com.mongodb.ReadConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.session.ClientSession;

import org.apache.commons.collections4.CollectionUtils;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Service
public class ChangeStreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamService.class);
    private static final String UPDATE = "UPDATE";
    private static final String DELETE = "DELETE";
    private static final String INSERT = "INSERT";
    private static final String FSKU_TOPIC = "flattenedsku_topic";
    private static final String PACKGESHPNG_DIMENSION_TOPIC = "packageshippingdimension_topic";
    private static final String PACKGESHPNG_DIMENSION = "skuPackageShippingDimensionList";

    //@Autowired
    CommonProperties commonProperties;

    //@Autowired
    KafkaProducer kafkaProducer;

    //@Autowired
    FlattenedSkuTypeConvertor flattenedSkuTypeConvertor;

    //@Autowired
    CSObjectRetriever cSObjectRetriever;

    //@Autowired
    MongoDatabase mongoDatabase;

    //@Autowired
    MongoClient client;


    public void setClient( MongoClient client) { this.client = client; }
    public void setMongoDatabase( MongoDatabase db) { this.mongoDatabase = db; }

    /**
     * This method frequently polls the collection for any document changes.
     *
     * @param collectionName
     *            String Name of the collection to watch for the change streams
     * @param session
     *            MongoDatabase mongo database object
     */
    @Async
    public void pollChangeStream(String collectionName, ClientSession session) {
        ObjectMapper mapper = new ObjectMapper();
        MongoCursor<ChangeStreamDocument<Document>> cursor = null;
        while (true) {
            try {
                if (session == null) {
                    ClientSessionOptions sessionOptions = ClientSessionOptions.builder().build();
                    session = client.startSession(sessionOptions);
                    LOGGER.info("Initialized session for collection: {}", collectionName);
                }
                if (cursor == null) {
                    cursor = getCursor(collectionName, session, mapper);
                    LOGGER.info("Initialized cursor for collection: {}", collectionName);
                }
                updateVersionCollection(collectionName, session, cursor);
                LOGGER.info("Restarting change stream watch for collection: {}", collectionName);
            } catch (Exception exc) {
                LOGGER.error("Exception while running change stream {}", exc);
            }
        }
    }

    private void updateVersionCollection(String collectionName, ClientSession session,
                                         MongoCursor<ChangeStreamDocument<Document>> cursor) throws Exception {
        String newCollection = collectionName + commonProperties.getCollectionSuffix();
        ChangeStreamDocument<Document> changeStreamDocument = null;
        ObjectMapper mapper = new ObjectMapper();
        boolean skipToken = false;
        int tokenCount = 0;
        List<Document> documents = new ArrayList<>();
        try {
            do {
                if (cursor != null) {
                    changeStreamDocument = cursor.next();
                    LOGGER.debug("Received change stream for collection {}, document {}", collectionName,
                            changeStreamDocument);
                    LOGGER.info("Resume token for collection: {} ,  {}", collectionName,
                            changeStreamDocument.getResumeToken());
                    if (null != changeStreamDocument && changeStreamDocument.getOperationType().getValue() != null) {
                        if (changeStreamDocument.getOperationType().getValue().equalsIgnoreCase(INSERT)) {
                            Document document = null;
                            Version version = new Version();
                            version.setIdDocument(Document.parse(changeStreamDocument.getDocumentKey().toJson()));
                            version.setInsertedFields(changeStreamDocument.getFullDocument());
                            try {
                                document = Document.parse(mapper.writeValueAsString(version));
                                documents.add(document);
                                LOGGER.debug("Adding document into the Collection: {} ,OT:Insert, IDDocument: {}",
                                        newCollection, changeStreamDocument.getDocumentKey().toJson());
                            } catch (Exception exc) {
                                skipToken = true;
                                LOGGER.error("Error while parsing the json: {} , Exception:  ", changeStreamDocument,
                                        exc);
                            }
                            if (document != null && !skipToken) {
                                tokenCount++;
                                if (commonProperties.isEnableFSKUKafka()) {
                                    LOGGER.debug("Sending inserted payload to kafka topic {}",
                                            changeStreamDocument.getFullDocument());
                                    sendToKafkaTopics(collectionName, changeStreamDocument.getFullDocument(), false);
                                }
                            } else {
                                LOGGER.error("Document is null or skipping token");
                            }
                        } else {
                            if (changeStreamDocument.getUpdateDescription() != null) {
                                String operationType = changeStreamDocument.getOperationType().getValue();
                                if (operationType.equalsIgnoreCase(UPDATE)) {
                                    Document document = null;
                                    String updateJson = null;
                                    boolean isPSD = false;
                                    Version version = new Version();
                                    version.setIdDocument(
                                            Document.parse(changeStreamDocument.getDocumentKey().toJson()));

                                    BsonDocument csDoc = changeStreamDocument.getUpdateDescription().getUpdatedFields();
                                    Set<String> keySet = csDoc.keySet();
                                    List<String> keyList = keySet.stream().filter(key -> key.contains("csId"))
                                            .collect(Collectors.<String>toList());
                                    if (collectionName.equalsIgnoreCase("sku")
                                            && (null != csDoc && csDoc.keySet().contains(PACKGESHPNG_DIMENSION))) {
                                        isPSD = true;
                                    }
                                    try {
                                        // TODO check the possibility of having csid as a list.
                                        keyList.forEach(key -> {
                                            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                                            BsonValue bsonValue = csDoc.get(key);
                                            if (null != bsonValue) {
                                                String keyString = bsonValue.asString().getValue();
                                                String[] keyValueString = keyString.split("-");
                                                Object csUpdatedEntity = cSObjectRetriever
                                                        .getCSUpdatedEntity(keyValueString);
                                                try {
                                                    version.setUpdatedFields(
                                                            Document.parse(mapper.writeValueAsString(csUpdatedEntity)));
                                                    LOGGER.debug("Converted json : "
                                                            + mapper.writeValueAsString(csUpdatedEntity));
                                                } catch (JsonProcessingException e) {
                                                    LOGGER.error(" Error parsing json: {}", e);
                                                }
                                            }
                                        });

                                        if (version.getUpdatedFields() == null) {
                                            updateJson = changeStreamDocument.getUpdateDescription().getUpdatedFields()
                                                    .toJson();
                                            version.setUpdatedFields(Document.parse(updateJson));
                                        } else {
                                            updateJson = version.getUpdatedFields().toJson();
                                        }
                                        version.setRemovedFields(
                                                changeStreamDocument.getUpdateDescription().getRemovedFields());
                                        document = Document.parse(mapper.writeValueAsString(version));
                                        LOGGER.debug(
                                                "Adding document into the Collection: {}, OT: Update, Document: {}",
                                                newCollection, changeStreamDocument.getDocumentKey().toJson());
                                        if (checkJsonValidity(changeStreamDocument.getDocumentKey().toJson())) {
                                            documents.add(document);
                                        } else {
                                            LOGGER.error("Error in {} resume token. Skipping it. {}", collectionName,
                                                    changeStreamDocument.getResumeToken());
                                            skipToken = true;
                                        }
                                    } catch (IllegalArgumentException illExc) {
                                        try {
                                            LOGGER.info(
                                                    "Calling jsonconverter before storing the document in version collection: {} , json: {}",
                                                    newCollection, updateJson);
                                            updateJson = JsonFormatter.formatJson(updateJson);
                                            version.setUpdatedFields(Document.parse(updateJson));
                                            document = Document.parse(mapper.writeValueAsString(version));
                                            LOGGER.debug(
                                                    "Adding document into the Collection: {}, OT: Update, Document: {}",
                                                    newCollection, changeStreamDocument.getDocumentKey().toJson());
                                            if (checkJsonValidity(changeStreamDocument.getDocumentKey().toJson())) {
                                                documents.add(document);
                                            } else {
                                                LOGGER.error("Error in {} resume token. Skipping it. {}",
                                                        collectionName, changeStreamDocument.getResumeToken());
                                                skipToken = true;
                                            }
                                        } catch (Exception e) {
                                            skipToken = true;
                                            LOGGER.error("Error while parsing the json: {} , Exception:  ",
                                                    changeStreamDocument, e);
                                        }
                                    } catch (Exception exc) {
                                        skipToken = true;
                                        LOGGER.error("Error while parsing the json: {} , Exception:  ",
                                                changeStreamDocument, exc);
                                    }
                                    if (document != null && !skipToken) {
                                        tokenCount++;
                                        if (commonProperties.isEnableFSKUKafka()) {
                                            LOGGER.debug("Sending updated payload to kafka topic: {}",
                                                    document.toJson());
                                            sendToKafkaTopics(collectionName, Document.parse(updateJson), isPSD);
                                        }
                                    } else {
                                        LOGGER.error("Document is null or skipping token");
                                    }
                                } else if (operationType.equalsIgnoreCase(DELETE)) {
                                    LOGGER.warn("Delete operation found {}", changeStreamDocument);
                                } else {
                                    LOGGER.warn("Operation is not handled by the PMDM Change Stream Application, {}",
                                            changeStreamDocument);
                                }
                            } else {
                                LOGGER.error("Updated fields is null: {}", changeStreamDocument);
                            }
                        }
                        if (tokenCount == commonProperties.getCsTokenCount()) {
                            storeResumeToken(changeStreamDocument.getResumeToken(), collectionName, session);
                            if (CollectionUtils.isNotEmpty(documents)) {
                                mongoDatabase.getCollection(newCollection).insertMany(session, documents);
                            } else {
                                LOGGER.error("No document to store");
                            }
                            tokenCount = 0;
                            documents = new ArrayList<>();
                        }
                    } else {
                        LOGGER.error("Cursor is null {}", changeStreamDocument);
                    }
                } else {
                    // This should not happen but adding to check for any errors.
                    LOGGER.error("Operation Type is null: {}", changeStreamDocument);
                }
            } while (cursor != null);
            LOGGER.error("Cursor is null.");
        } catch (Exception exc) {
            LOGGER.error("Exception in change stream {}", exc);
        } finally {
            if (cursor != null) {
                LOGGER.info("Closing cursor explicitly for {}", collectionName);
                cursor.close();
                cursor = null;
            }
            if (session != null) {
                LOGGER.info("Closing session explicitly for {}", collectionName);
                session.close();
                session = null;
            }
        }
    }

    private boolean checkJsonValidity(String jsonString) {
        Gson gson = new Gson();
        try {
            gson.fromJson(jsonString, Document.class);
            return true;
        } catch (Exception ex) {
            LOGGER.error("Error in json Document: {} ", jsonString);
            return false;
        }
    }

    private void storeResumeToken(BsonDocument resumeToken, String collectionName, ClientSession session) {
        Sequence sequence = new Sequence();
        sequence.set_id(collectionName + Constants.TOKEN);
        sequence.setBsonToken(resumeToken.toJson());
        ObjectMapper mapper = new ObjectMapper();
        LOGGER.info("Writing resume token of collection {} into DB: {}", collectionName, resumeToken.toJson());
        BasicDBObject whereQuery = new BasicDBObject();
        whereQuery.put(Constants.ID, collectionName + Constants.TOKEN);
        try {
            mongoDatabase.getCollection("sequence").findOneAndReplace(session, whereQuery,
                    Document.parse(mapper.writeValueAsString(sequence)));
        } catch (JsonProcessingException e) {
            LOGGER.error("JSON parsing error {}", e);
        }
    }

    private MongoCursor<ChangeStreamDocument<Document>> getCursor(String collectionName, ClientSession session,
                                                                  ObjectMapper mapper) throws Exception {
        LOGGER.info("Initializing cursor");
        MongoCollection<Document> collection = mongoDatabase.withCodecRegistry(getPojoCodecRegistry(collectionName))
                .getCollection(collectionName).withReadConcern(ReadConcern.MAJORITY);
        MongoCursor<ChangeStreamDocument<Document>> cursor = null;
        BsonDocument resumeToken;
        // During app restart this will be called first to poll the change stream with
        // resume token.
        BasicDBObject whereQuery = new BasicDBObject();
        whereQuery.put(Constants.ID, collectionName + Constants.TOKEN);
        FindIterable<Document> docItr = mongoDatabase.getCollection("sequence").find(session, whereQuery);
        Document seqDocument = docItr.first();
        if (seqDocument != null) {
            Sequence sequence = new Sequence();
            try {
                sequence = mapper.readValue(seqDocument.toJson(), Sequence.class);
            } catch (IOException e) {
                LOGGER.error("Exception while retrieving the object from DB: {}", e);
            }
            if (sequence.getBsonToken() != null) {
                resumeToken = BsonDocument.parse(sequence.getBsonToken());
                try {
                    cursor = collection.watch(session).resumeAfter(resumeToken).iterator();
                } catch (Exception mce) {
                    LOGGER.error("Resume token error so clearing the resume token to proceed further, exception: {}",
                            mce);
                    mongoDatabase.getCollection("sequence").findOneAndReplace(session, whereQuery, new Document());
                    cursor.next();
                }
            } else {
                cursor = collection.watch(session).iterator();
                LOGGER.info("Sequence resume token is null initializing cursor");
            }
        } else {
            cursor = collection.watch(session).iterator();
            LOGGER.info("Sequence document is null initializing cursor");
        }
        return cursor;
    }

    private void sendToKafkaTopics(String collectionName, Document updateJsonDoc, boolean isPSD) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        mapper.setDateFormat(df);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // Converting updated json to FlattenedSkuType.
        try {
            if (!isPSD) {
                // No need to send FSKU incase of PSD changes.
                FlattenedSKUType flattenedSKUType = flattenedSkuTypeConvertor.convertToFlattenedSkuType(collectionName,
                        mapper.writeValueAsString(updateJsonDoc));
                kafkaProducer.produce(FSKU_TOPIC, mapper.writeValueAsString(flattenedSKUType));
            } else {
                if (null != updateJsonDoc.get("skuPackageShippingDimensionList")) {
                    Document skuPckgDoc = getFormattedSkuPkg(updateJsonDoc);
                    kafkaProducer.produce(PACKGESHPNG_DIMENSION_TOPIC, new Document("sku", skuPckgDoc).toJson());
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error while converting into Flattened sku type, continuing change stream. ", e);
        }
    }

    private Document getFormattedSkuPkg(Document updateJsonDoc) {

        List<Document> updatedSkuPkgList = (List<Document>) updateJsonDoc.get("skuPackageShippingDimensionList");
        List<Document> formattedSkupkgList = new ArrayList<>();

        Document skuPckgDoc = new Document();
        skuPckgDoc.append("skuNumber", updatedSkuPkgList.get(0).get("skuNumber").toString());
        Document packageDimensionDoc = new Document();
        packageDimensionDoc.append("depthQuantity", updatedSkuPkgList.get(0).get("depthQuantity"));
        packageDimensionDoc.append("heightQuantity", updatedSkuPkgList.get(0).get("heightQuantity"));

        Document heightWidthDepthUnitOfMeasureDoc = (Document) updatedSkuPkgList.get(0)
                .get("heightWidthDepthUnitOfMeasure");
        packageDimensionDoc.append("heightWidthDepthUnitOfMeasureCode",
                heightWidthDepthUnitOfMeasureDoc.get("referenceValueShortDescription"));

        if (null != updatedSkuPkgList.get(0).get("modifiedDateTime")) {
            try {

                Date modifiedDate = (Date) updatedSkuPkgList.get(0).get("modifiedDateTime");
                DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                String formatted = format.format(modifiedDate);

                packageDimensionDoc.append("modifiedDateTime", formatted);
            } catch (Exception e) {
                try {
                    String modifiedDate = (String) updatedSkuPkgList.get(0).get("modifiedDateTime");
                    modifiedDate = modifiedDate.replace("{\"$date\":", "");
                    modifiedDate = modifiedDate.replace("}", "");

                    Date date = new Date(Long.parseLong(modifiedDate));
                    DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                    String formatted = format.format(date);

                    packageDimensionDoc.append("modifiedDateTime", formatted);
                } catch (Exception exc) {
                    LOGGER.debug("Not able to parse the date");
                }
            }
        }
        packageDimensionDoc.append("weightQuantity", updatedSkuPkgList.get(0).get("weightQuantity"));

        Document weightUnitOfMeasureDoc = (Document) updatedSkuPkgList.get(0).get("weightUnitOfMeasure");
        packageDimensionDoc.append("weightUnitOfMeasureCode",
                weightUnitOfMeasureDoc.get("referenceValueShortDescription"));
        packageDimensionDoc.append("widthQuantity", updatedSkuPkgList.get(0).get("widthQuantity"));
        formattedSkupkgList.add(packageDimensionDoc);
        skuPckgDoc.append("skuPackageShippingDimensionList", formattedSkupkgList);

        return skuPckgDoc;
    }

    private CodecRegistry getPojoCodecRegistry(String collectionName) {
        CodecRegistry pojoCodecRegistry = null;
        if (collectionName.equalsIgnoreCase("sku")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(SKU.class).build()));
        } else if (collectionName.equalsIgnoreCase("kohlsstyle")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(KohlsStyle.class).build()));
        } else if (collectionName.equalsIgnoreCase("consumerproduct")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(Product.class).build()));
        } else if (collectionName.equalsIgnoreCase("attributenamecrossreference")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(AttributeNameCrossReference.class).build()));
        } else if (collectionName.equalsIgnoreCase("brand")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(Brand.class).build()));
        } else if (collectionName.equalsIgnoreCase("businessgroup")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(BusinessGroup.class).build()));
        } else if (collectionName.equalsIgnoreCase("buyingcontact")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(BuyingContact.class).build()));
        } else if (collectionName.equalsIgnoreCase("calendar")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(Calendar.class).build()));
        } else if (collectionName.equalsIgnoreCase("color")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(Color.class).build()));
        } else if (collectionName.equalsIgnoreCase("country")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(Country.class).build()));
        } else if (collectionName.equalsIgnoreCase("customerchoicebirthstone")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(CustomerChoiceBirthStone.class).build()));
        } else if (collectionName.equalsIgnoreCase("department")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(Department.class).build()));
        } else if (collectionName.equalsIgnoreCase("operatingcompany")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(OperatingCompany.class).build()));
        } else if (collectionName.equalsIgnoreCase("packagingspecificationgrid")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(PackagingSpecificationGrid.class).build()));
        } else if (collectionName.equalsIgnoreCase("referencecode")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(ReferenceCode.class).build()));
        } else if (collectionName.equalsIgnoreCase("reportinglabelpurpose")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(ReportingLabelPurpose.class).build()));
        } else if (collectionName.equalsIgnoreCase("size")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(Size.class).build()));
        } else if (collectionName.equalsIgnoreCase("vendor")) {
            pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().register(Vendor.class).build()));
        }
        return pojoCodecRegistry;
    }
}
