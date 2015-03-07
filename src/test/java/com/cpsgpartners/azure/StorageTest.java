package com.cpsgpartners.azure;

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Test;
import org.w3c.dom.Document;

import com.cpsgpartners.azure.storage.Storage;
import com.cpsgpartners.azure.storage.Storage.BlobFile;

public class StorageTest {

	public static final String MASTER_KEY = "";
	public static final String CLIENT_ID = "";

	/*//@Test
	public void testMSCreateCollection() throws Exception {
		System.setProperty("http.proxyHost", "127.0.0.1");
		System.setProperty("https.proxyHost", "127.0.0.1");
		System.setProperty("http.proxyPort", "8080");
		System.setProperty("https.proxyPort", "8080");

		String storageConnectionString = "";
		CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
		CloudBlobClient serviceClient = account.createCloudBlobClient();
		CloudBlobContainer container = serviceClient.getContainerReference("junittest");
		container.createIfNotExists();
		CloudBlockBlob blob = container.getBlockBlobReference("image1.jpg");
		File sourceFile = new File("c:\\myimages\\image1.jpg");
		blob.upload(new FileInputStream(sourceFile), sourceFile.length());
		container.delete();
	}*/

	@Test
	public void testStorage() throws Exception {
		//fpr mitmproxy tracing
		/*System.setProperty("http.proxyHost", "127.0.0.1");
		System.setProperty("https.proxyHost", "127.0.0.1");
		System.setProperty("http.proxyPort", "8080");
		System.setProperty("https.proxyPort", "8080");*/

		Storage storage = new Storage(CLIENT_ID, MASTER_KEY);
		try {
			storage.createContainer("testcontainer");
			Response contResp = storage.getContainer("testcontainer");
			assertNotNull(contResp);
			contResp.close();
			ByteArrayInputStream bis = new ByteArrayInputStream("Test Contents".getBytes());
			storage.createOrUpdateBlob("testcontainer", "testblob", new BlobFile(bis, "test.txt", MediaType.TEXT_PLAIN));
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(storage.listContainers());
			assertNotNull(doc);
			BlobFile blob = storage.getBlob("testcontainer", "testblob");
			java.util.Scanner s = new java.util.Scanner(blob.content).useDelimiter("\\A");
			//System.out.format("File: %s - %s\n", blob.fileName, s.next());
			assertNotNull(blob.fileName);
			assertNotNull(s.next());
			System.out.format("Shared URL: %s\n", storage.sharedAccessURL("testcontainer", "testblob", "r", null, null, false));
			storage.deleteContainer("testcontainer");
			//System.out.format("Container Delete\n");
		} catch (WebApplicationException we) {
			String message = we.getMessage();
			if (we.getResponse().hasEntity()) {
				message = String.format("%s - %s", message, we.getResponse().readEntity(String.class));
			}
			try {
				storage.deleteContainer("testcontainer");
			} catch (Exception e) {
			}
			org.junit.Assert.fail(message);
		}
	}
}
