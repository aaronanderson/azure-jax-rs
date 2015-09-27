package com.cpsgpartners.azure;

import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;
import java.util.Properties;

import javax.json.Json;
import javax.json.JsonObject;
import javax.ws.rs.WebApplicationException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cpsgpartners.azure.search.Search;
import com.cpsgpartners.azure.search.Search.SearchQuery;
import com.cpsgpartners.azure.search.Search.SuggestQuery;

public class SearchTest {

	public static String MASTER_KEY;
	public static String CLIENT_ID;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String fileName = System.getProperty("cpsg.test.cfg.filename");
		//System.out.format("Reading config file : %s\n", fileName);
		Properties p = new Properties();
		try (FileInputStream fis = new FileInputStream(fileName)) {
			p.load(fis);
			MASTER_KEY = p.getProperty("search.masterkey");
			CLIENT_ID = p.getProperty("search.clientid");
		}
	}

	@Test
	public void testSearch() {
		Search search = new Search(CLIENT_ID, MASTER_KEY);
		try {

			JsonObject index = Json
					.createObjectBuilder()
					.add("name", "hotels")
					.add("fields",
							Json.createArrayBuilder().add(Json.createObjectBuilder().add("name", "hotelId").add("type", "Edm.String").add("key", true))
									.add(Json.createObjectBuilder().add("name", "description").add("type", "Edm.String")))
					.add("suggesters",
							Json.createArrayBuilder().add(
									Json.createObjectBuilder().add("name", "sg").add("searchMode", "analyzingInfixMatching").add("sourceFields", Json.createArrayBuilder().add("description")))).build();
			index = search.createIndex(index);
			//System.out.format("Create index %s\n", index);
			assertNotNull(index);
			index = search.getIndex("hotels");
			//System.out.format("Get index %s\n", index);
			assertNotNull(index);
			index = search.updateIndex("hotels", index);
			//System.out.format("Update index %s\n", index);
			index = search.listIndexes();
			//System.out.format("List index %s\n", index);
			assertNotNull(index);

			JsonObject documents = Json.createObjectBuilder()
					.add("value", Json.createArrayBuilder().add(Json.createObjectBuilder().add("@search.action", "upload").add("hotelId", "1").add("description", "Marriot"))).build();
			//System.out.format("Lookup document  before %s\n", documents);
			JsonObject result = search.processDocuments("hotels", documents);
			//System.out.format("Uploaded document %s\n", result);
			assertNotNull(result);
			result = search.lookupDocument("hotels", "1");
			//System.out.format("Lookup document %s\n", result);
			assertNotNull(result);
			result = search.searchDocuments("hotels", new SearchQuery().setSearch("*"));
			//System.out.format("Search document %s\n", result);
			assertNotNull(result);
			result = search.suggestDocuments("hotels", new SuggestQuery().setSuggesterName("sg").setSearch("mar"));
			//System.out.format("Suggest document %s\n", result);
			assertNotNull(result);

			search.deleteIndex("hotels");
			//System.out.format("Delete index %s\n", index);
		} catch (WebApplicationException we) {
			we.printStackTrace();
			String message = we.getMessage();
			if (we.getResponse().hasEntity()) {
				message = String.format("%s - %s", message, we.getResponse().readEntity(JsonObject.class));
			}
			try {
				search.deleteIndex("hotels");
			} catch (WebApplicationException e) {
			}
			org.junit.Assert.fail(message);
		}

	}
}
