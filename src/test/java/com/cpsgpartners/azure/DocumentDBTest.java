package com.cpsgpartners.azure;

import static org.junit.Assert.assertNotNull;

import javax.json.Json;
import javax.json.JsonObject;
import javax.ws.rs.WebApplicationException;

import org.junit.Test;

import com.cpsgpartners.azure.documentdb.DocumentDB;
import com.cpsgpartners.azure.documentdb.DocumentDB.QueryResult;

public class DocumentDBTest {

	public static final String MASTER_KEY = "";
	public static final String CLIENT_ID = "";

	/*
	//@Test
	public void testMSCreateCollection() throws Exception {	

		DocumentClient documentClient = new DocumentClient("https://client.documents.azure.com", MASTER_KEY, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);

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

	@Test
	public void testDocumentDB() throws Exception {
		DocumentDB documentDB = new DocumentDB(CLIENT_ID, MASTER_KEY, com.cpsgpartners.azure.documentdb.DocumentDB.ConsistencyLevel.Session);
		String dbResId = null;
		try {
			JsonObject documentDb = documentDB.createDatabase("TestDB");
			//System.out.format("Create Database %s\n", documentDb);
			assertNotNull(documentDB);
			dbResId = documentDb.getString("_rid");

			JsonObject collection = documentDB.createCollection(documentDb.getString("_rid"), "TestCollectionID");
			//System.out.format("Create Collection %s\n", collection);
			assertNotNull(collection);

			JsonObject collectionList = documentDB.listCollections(documentDb.getString("_rid"));
			//System.out.format("List Collection %s\n", collectionList);
			assertNotNull(collectionList);

			JsonObject document = Json.createObjectBuilder().add("id", "TestDocumentID").add("foo", "bar").build();
			document = documentDB.createDocument(documentDb.getString("_rid"), collection.getString("_rid"), document);
			//System.out.format("Create Document %s\n", document);
			assertNotNull(document);

			JsonObject documentList = documentDB.listDocuments(documentDb.getString("_rid"), collection.getString("_rid"));
			//System.out.format("List Documents %s\n", documentList);
			assertNotNull(documentList);

			JsonObject newDocument = Json.createObjectBuilder().add("id", "TestDocumentID").add("foo2", "bar2").build();
			document = documentDB.replaceDocument(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"), newDocument);
			//System.out.format("Replace Document %s\n", document);
			assertNotNull(document);

			document = documentDB.getDocument(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"));
			//System.out.format("Get Document %s\n", document);
			assertNotNull(document);

			//System.out.format("Query Documents\n");
			QueryResult qResult = documentDB.queryDocuments(documentDb.getString("_rid"), collection.getString("_rid"), "SELECT * FROM c", -1, null);
			assertNotNull(qResult);
			for (JsonObject qdoc : qResult) {
				//System.out.format("\tQuery Result %s\n", qdoc);
			}

			JsonObject attachment = documentDB.createAttachment(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"), "TestAttachmentID", "text.txt", "text/plain",
					"https://azure/test.txt");
			//System.out.format("Create Attachment %s\n", attachment);
			assertNotNull(attachment);

			JsonObject attachementList = documentDB.listAttachments(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"));
			//System.out.format("List Attachments %s\n", attachementList);
			assertNotNull(attachementList);

			attachment = documentDB.replaceAttachment(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"), attachment.getString("_rid"), "text.txt",
					"TestAttachmentID", "text/plain", "https://azure/test.txt");
			//System.out.format("Replace Attachment %s\n", attachment);
			assertNotNull(attachment);

			attachment = documentDB.getAttachment(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"), attachment.getString("_rid"));
			//System.out.format("Get Attachment %s\n", attachment);
			assertNotNull(attachment);

			documentDB.deleteAttachment(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"), attachment.getString("_rid"));
			//System.out.format("Delete Attachment %s\n", attachment.getString("_rid"));

			documentDB.deleteDocument(documentDb.getString("_rid"), collection.getString("_rid"), document.getString("_rid"));
			//System.out.format("Delete Document %s\n", document.getString("id"));

			documentDB.deleteDatabase(documentDb.getString("_rid"));
			//System.out.format("Delete Database\n");

		} catch (WebApplicationException we) {
			String message = we.getMessage();
			if (we.getResponse().hasEntity()) {
				message = String.format("%s - %s", message, we.getResponse().readEntity(JsonObject.class));
			}
			try {
				documentDB.deleteDatabase(dbResId);
			} catch (WebApplicationException e) {
			}
			org.junit.Assert.fail(message);
		}

	}
}
