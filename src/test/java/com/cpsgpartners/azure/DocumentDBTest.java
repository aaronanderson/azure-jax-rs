package com.cpsgpartners.azure;

import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.ContextResolver;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cpsgpartners.azure.documentdb.DocumentDB;
import com.cpsgpartners.azure.documentdb.DocumentDB.QueryResult;
import com.cpsgpartners.azure.documentdb.JQueryResult;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DocumentDBTest {
	private static String MASTER_KEY;
	private static String CLIENT_ID;

	/*
	//@Test
	public void testMSCreateCollection() throws Exception {
	  Properties props = new Properties();
	props.setProperty("log4j.rootLogger", "INFO, stdout");
	props.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
	props.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
	props.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%5p [%t] (%c) - %m%n");
	PropertyConfigurator.configure(props);
	
		System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
	
		System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
	
		System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire.header", "debug");
	
		System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.commons.httpclient", "debug");
	
		DocumentClient documentClient = new DocumentClient("https://cpsgpartners.documents.azure.com", MASTER_KEY, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);
	
		// Define a new database using the id above.
		Database myDatabase = new Database();
		myDatabase.setId("junitDB");
	
		// Create a new database.
		myDatabase = documentClient.createDatabase(myDatabase, null).getResource();
	
		// Define a new collection using the id above.
		DocumentCollection myCollection = new DocumentCollection();
		myCollection.setId("TestCollectionID");
	
		// Create a new collection.
		//myCollection = documentClient.createCollection(myDatabase.getSelfLink(), myCollection, null).getResource();
		myCollection = documentClient.createCollection(myDatabase.getSelfLink(), myCollection, null).getResource();
	
		JsonObject jobj = Json.createObjectBuilder().add("foo", "bar").build();
		Document myDocument = new Document(jobj.toString());
	
		// Create a new document.
		myDocument = documentClient.createDocument(myCollection.getSelfLink(), myDocument, null, false).getResource();
	
		FeedOptions fo = new FeedOptions();
		fo.setPageSize(30);
		documentClient.readDocuments(myCollection.getSelfLink(), fo);
	
		FeedResponse<Document> res = documentClient.queryDocuments(myCollection.getSelfLink(), "SELECT * FROM c", fo);
		for (Document doc : res.getQueryIterable()) {
			System.out.println(doc.toString());
		}
		RequestOptions ro = new RequestOptions();
	
		documentClient.deleteDatabase(myDatabase.getSelfLink(), ro);
	
	}*/

	public static class ObjectMapperContextResolver implements ContextResolver<ObjectMapper> {
		private ObjectMapper mapper = null;

		public ObjectMapperContextResolver() {
			mapper = new ObjectMapper();//.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		}

		@Override
		public ObjectMapper getContext(Class<?> type) {
			return mapper;
		}
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class SerTest implements Serializable {

		private String id;

		private String foo;
		private int bar;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getFoo() {
			return foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

		public int getBar() {
			return bar;
		}

		public void setBar(int bar) {
			this.bar = bar;
		}

	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String fileName = System.getProperty("cpsg.test.cfg.filename");
		//System.out.format("Reading config file : %s\n", fileName);
		Properties p = new Properties();
		try (FileInputStream fis = new FileInputStream(fileName)) {
			p.load(fis);
			MASTER_KEY = p.getProperty("documentdb.masterkey");
			CLIENT_ID = p.getProperty("documentdb.clientid");
		}
	}

	@Test
	public void testDocumentDB() throws Exception {
		DocumentDB documentDB = new DocumentDB(CLIENT_ID, MASTER_KEY, com.cpsgpartners.azure.documentdb.DocumentDB.ConsistencyLevel.Session, ObjectMapperContextResolver.class);
		String dbResId = null;
		try {

			JsonObject documentDb = documentDB.createDatabase("TestDB");
			//System.out.format("Create Database %s\n", documentDb);
			assertNotNull(documentDB);
			dbResId = documentDb.getString("_rid");

			documentDb = documentDB.getDatabase(dbResId);
			//System.out.format("Get Database %s\n", documentDb);
			assertNotNull(documentDB);

			JsonObject documentDbList = documentDB.listDatabases();
			//System.out.format("Create Database %s\n", documentDb);
			assertNotNull(documentDbList);

			JsonObject collection = documentDB.createCollection(documentDb.getString("_rid"), "TestCollectionID");
			//System.out.format("Create Collection %s\n", collection);
			assertNotNull(collection);

			JsonObject collectionList = documentDB.listCollections(documentDb.getString("_rid"));
			//System.out.format("List Collection %s\n", collectionList);
			assertNotNull(collectionList);

			JsonObject document = Json.createObjectBuilder().add("id", "TestDocumentID").add("foo", "bar").build();
			document = documentDB.createDocument(documentDb.getString("_rid"), collection.getString("_rid"), document, JsonObject.class, null);
			//System.out.format("Create Document %s\n", document);
			assertNotNull(document);

			JsonObject documentList = documentDB.listDocuments(documentDb.getString("_rid"), collection.getString("_rid"), JsonObject.class);
			//System.out.format("List Documents %s\n", documentList);
			assertNotNull(documentList);

			JsonObject newDocument = Json.createObjectBuilder().add("id", "TestDocumentID").add("foo2", "bar2").build();
			document = documentDB.replaceDocument(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"), newDocument, JsonObject.class, null);
			//System.out.format("Replace Document %s\n", document);
			assertNotNull(document);

			document = documentDB.getDocument(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"), JsonObject.class);
			//System.out.format("Get Document %s\n", document);
			assertNotNull(document);

			SerTest serTest = new SerTest();
			serTest.setId("TestInstanceID1");
			serTest.setFoo("FooValue1");
			serTest.setBar(10);
			document = documentDB.createDocument(documentDb.getString("_rid"), collection.getString("_rid"), serTest, JsonObject.class, null);
			//System.out.format("Create Object %s\n", document);
			assertNotNull(document);

			serTest = new SerTest();
			serTest.setId("TestInstanceID2");
			serTest.setFoo("FooValue2");
			serTest.setBar(20);
			serTest = documentDB.createDocument(documentDb.getString("_rid"), collection.getString("_rid"), serTest, SerTest.class, null);
			//System.out.format("Create Object %s\n", serTest.getId());
			assertNotNull(document);

			//System.out.format("Query Documents\n");
			//System.out.format("%s\n", documentDB.queryDocuments(documentDb.getString("_rid"), collection.getString("_rid"), "SELECT * FROM c", String.class, -1, null));
			QueryResult<SerTest> qResult = documentDB.queryDocuments(documentDb.getString("_rid"), collection.getString("_rid"), "SELECT * FROM c", new HashMap<String, String>(), JQueryResult.genericType(SerTest.class), -1, null);
			assertNotNull(qResult);

			/*for (SerTest qdoc : qResult.getDocuments()) {
				System.out.format("\tQuery Result %s\n", qdoc.getId());
			}*/
			JsonObject extraData = Json.createObjectBuilder().add("CustomKey", "CustomValue").build();
			JsonObject attachment = documentDB.createAttachment(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"), "TestAttachmentID", "text.txt", "text/plain", "https://azure/test.txt", extraData);
			//System.out.format("Create Attachment %s\n", attachment);
			assertNotNull(attachment);

			JsonObject attachementList = documentDB.listAttachments(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"));
			//System.out.format("List Attachments %s\n", attachementList);
			assertNotNull(attachementList);

			attachment = documentDB.replaceAttachment(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"), attachment.getString("_rid"), "text.txt", "TestAttachmentID", "text/plain", "https://azure/test.txt",extraData);
			//System.out.format("Replace Attachment %s\n", attachment);
			assertNotNull(attachment);

			attachment = documentDB.getAttachment(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"), attachment.getString("_rid"));
			//System.out.format("Get Attachment %s\n", attachment);
			assertNotNull(attachment);

			documentDB.deleteAttachment(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"), attachment.getString("_rid"));
			//System.out.format("Delete Attachment %s\n", attachment.getString("_rid"));

			documentDB.deleteDocument(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"));
			//System.out.format("Delete Document %s\n", document.getString("id"));

			StringBuilder spBody = new StringBuilder();
			spBody.append("function (parm) {\n");
			spBody.append("    var context = getContext();\n");
			spBody.append("    var response = context.getResponse();\n");
			spBody.append("    response.setBody({ id: \"Hello, World \" + parm });\n");
			spBody.append("}\n");
			JsonObject sproc = documentDB.createStoredProcedure(documentDb.getString("_rid"), collection.getString("_rid"), "TestSP", spBody.toString());
			//System.out.format("Create Stored Procedure %s\n", sproc);
			assertNotNull(sproc);

			JsonObject sprocList = documentDB.listStoredProcedures(documentDb.getString("_rid"), collection.getString("_rid"));
			//System.out.format("Stored Procedure List %s\n", sprocList);
			assertNotNull(sprocList);

			sproc = documentDB.replaceStoredProcedure(documentDb.getString("_rid"), collection.getString("_rid"), sproc.getString("_rid"), "TestSP", spBody.toString());
			//System.out.format("Replace Stored Procedure %s\n", sproc);
			assertNotNull(sproc);

			JsonArray params = Json.createArrayBuilder().add("Client Value").build();
			JsonObject sprocExec = documentDB.executeStoredProcedure(documentDb.getString("_rid"), collection.getString("_rid"), sproc.getString("_rid"), params, JsonObject.class);
			//System.out.format("Exec Stored Procedure %s\n", sproc);
			assertNotNull(sprocExec);

			StringBuilder udfBody = new StringBuilder();
			udfBody.append("function (parm) {\n");
			udfBody.append("    return parm; \n");
			udfBody.append("}\n");
			JsonObject udf = documentDB.createUDF(documentDb.getString("_rid"), collection.getString("_rid"), "testUDF", udfBody.toString());
			//System.out.format("Create UDF %s\n", udf);
			assertNotNull(udf);

			JsonObject udfList = documentDB.listUDFs(documentDb.getString("_rid"), collection.getString("_rid"));
			//System.out.format("User Defined Function List %s\n", udfList);
			assertNotNull(udfList);

			udf = documentDB.replaceUDF(documentDb.getString("_rid"), collection.getString("_rid"), udf.getString("_rid"), "testUDF", udfBody.toString());
			//System.out.format("Replace Stored Procedure %s\n", udf);
			assertNotNull(udf);

			JsonObject udfResult = documentDB.queryDocuments(documentDb.getString("_rid"), collection.getString("_rid"), "SELECT udf.testUDF('Test')", new HashMap<String, String>(), JsonObject.class, -1, null);
			//System.out.format("Exec UDF %s\n", udfResult);
			assertNotNull(udfResult);

			documentDB.deleteDatabase(documentDb.getString("_rid"));
			//System.out.format("Delete Database\n");

		} catch (ProcessingException pe) {
			pe.getCause().printStackTrace();
		} catch (WebApplicationException we) {
			String message = we.getMessage();
			if (we.getResponse().hasEntity()) {
				message = String.format("%s - %s", message, we.getResponse().readEntity(JsonObject.class));
				we.printStackTrace();
			}
			try {
				documentDB.deleteDatabase(dbResId);
			} catch (WebApplicationException e) {
			}
			org.junit.Assert.fail(message);
		}

	}
}
