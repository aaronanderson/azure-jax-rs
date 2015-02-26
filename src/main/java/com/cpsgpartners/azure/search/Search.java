package com.cpsgpartners.azure.search;

import javax.json.JsonObject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

//import org.glassfish.jersey.filter.LoggingFilter;

public class Search {
	public static final String AZURE_DOCUMENTDB_ENDPOINT = "https://%s.search.windows.net";
	public static final String AZURE_SEARCH_VERSION = "2014-07-31-Preview";

	private final String masterKey;
	private final Client client;
	private final WebTarget endpoint;

	public Search(String id, String masterKey) {
		this.masterKey = masterKey;
		client = ClientBuilder.newClient();
		//client.register(new LoggingFilter());
		endpoint = client.target(String.format(AZURE_DOCUMENTDB_ENDPOINT, id));
	}

	public JsonObject createIndex(JsonObject index) throws WebApplicationException {
		return operation(endpoint.path("indexes").queryParam("api-version", AZURE_SEARCH_VERSION), "POST", Response.Status.CREATED.getStatusCode(),
				Entity.entity(index, MediaType.APPLICATION_JSON_TYPE));
	}

	public JsonObject getIndex(String indexName) throws WebApplicationException {
		return operation(endpoint.path("indexes").path(indexName).queryParam("api-version", AZURE_SEARCH_VERSION), "GET", Response.Status.OK.getStatusCode(), null);
	}

	public JsonObject listIndexes() throws WebApplicationException {
		return operation(endpoint.path("indexes").queryParam("api-version", AZURE_SEARCH_VERSION), "GET", Response.Status.OK.getStatusCode(), null);
	}

	public JsonObject updateIndex(String indexName, JsonObject index) throws WebApplicationException {
		return operation(endpoint.path("indexes").path(indexName).queryParam("api-version", AZURE_SEARCH_VERSION), "PUT", Response.Status.NO_CONTENT.getStatusCode(),
				Entity.entity(index, MediaType.APPLICATION_JSON_TYPE));
	}

	public void deleteIndex(String indexName) throws WebApplicationException {
		operation(endpoint.path("indexes").path(indexName).queryParam("api-version", AZURE_SEARCH_VERSION), "DELETE", Response.Status.NO_CONTENT.getStatusCode(), null);
	}

	//available actions: upload,merge,mergeOrUpload,delete
	public JsonObject processDocuments(String indexName, JsonObject documents) throws WebApplicationException {
		return operation(endpoint.path("indexes").path(indexName).path("docs/index").queryParam("api-version", AZURE_SEARCH_VERSION), "POST", Response.Status.OK.getStatusCode(),
				Entity.entity(documents, MediaType.APPLICATION_JSON_TYPE));
	}

	public JsonObject lookupDocument(String indexName, String docKey, String... fields) throws WebApplicationException {
		WebTarget lookup = endpoint.path("indexes").path(indexName).path("docs").path(docKey).queryParam("api-version", AZURE_SEARCH_VERSION);
		if (fields.length > 0) {
			lookup = lookup.queryParam("$select", toCSV(fields));
		}
		return operation(lookup, "GET", Response.Status.OK.getStatusCode(), null);
	}

	public static class SearchQuery {

		public static enum Mode {
			ANY, ALL;
		}

		String search;
		Mode searchMode;
		String searchFields;
		int skip = -1;
		int top = -1;
		Boolean count;
		String orderBy;
		String select;
		String facet;
		String filter;
		String highlight;
		String scoringProfile;
		String scoringParameter;

		public SearchQuery setSearch(String search) {
			this.search = search;
			return this;
		}

		public SearchQuery setSearchMode(Mode searchMode) {
			this.searchMode = searchMode;
			return this;
		}

		public SearchQuery setSearchFields(String... searchFields) {
			this.searchFields = toCSV(searchFields);
			return this;
		}

		public SearchQuery setSkip(int skip) {
			this.skip = skip;
			return this;
		}

		public SearchQuery setTop(int top) {
			this.top = top;
			return this;
		}

		public SearchQuery setCount(boolean count) {
			this.count = count;
			return this;
		}

		public SearchQuery setOrderBy(String... fields) {
			this.orderBy = toCSV(fields);
			return this;
		}

		public SearchQuery setSelect(String... fields) {
			this.select = toCSV(fields);
			return this;
		}

		public SearchQuery setFacet(String facet) {
			this.facet = facet;
			return this;
		}

		public SearchQuery setFilter(String filter) {
			this.filter = filter;
			return this;
		}

		public SearchQuery setHighlight(String... fields) {
			this.highlight = toCSV(fields);
			return this;
		}

		public SearchQuery setScoringProfile(String scoringProfile) {
			this.scoringProfile = scoringProfile;
			return this;
		}

		public SearchQuery setScoringParameter(String scoringParameter) {
			this.scoringParameter = scoringParameter;
			return this;
		}

	}

	public JsonObject searchDocuments(String indexName, SearchQuery query) throws WebApplicationException {
		WebTarget lookup = endpoint.path("indexes").path(indexName).path("docs");
		if (query.search != null) {
			lookup = lookup.queryParam("search", query.search);
		}

		if (query.searchMode != null) {
			lookup = lookup.queryParam("searchMode", query.searchMode.name().toLowerCase());
		}
		if (query.searchFields != null) {
			lookup = lookup.queryParam("searchFields", query.searchFields);
		}
		if (query.skip > -1) {
			lookup = lookup.queryParam("$skip", query.skip);
		}
		if (query.top > -1) {
			lookup = lookup.queryParam("$top", query.top);
		}
		if (query.count != null) {
			lookup = lookup.queryParam("$count", query.count);
		}
		if (query.orderBy != null) {
			lookup = lookup.queryParam("$orderby", query.orderBy);
		}
		if (query.select != null) {
			lookup = lookup.queryParam("$select", query.select);
		}
		if (query.facet != null) {
			lookup = lookup.queryParam("facet", query.facet);
		}
		if (query.filter != null) {
			lookup = lookup.queryParam("$filter", query.filter);
		}
		if (query.highlight != null) {
			lookup = lookup.queryParam("highlight", query.highlight);
		}
		if (query.facet != null) {
			lookup = lookup.queryParam("scoringProfile", query.scoringProfile);
		}
		if (query.facet != null) {
			lookup = lookup.queryParam("scoringParameter", query.scoringParameter);
		}

		lookup = lookup.queryParam("api-version", AZURE_SEARCH_VERSION);

		return operation(lookup, "GET", Response.Status.OK.getStatusCode(), null);
	}

	public static class SuggestQuery {

		public static enum Mode {
			ANY, ALL;
		}

		String search;
		Boolean fuzzy;
		String searchFields;
		int top = -1;
		String filter;
		String orderBy;
		String select;

		public SuggestQuery setSearch(String search) {
			this.search = search;
			return this;
		}

		public SuggestQuery setFuzzy(boolean fuzzy) {
			this.fuzzy = fuzzy;
			return this;
		}

		public SuggestQuery setSearchFields(String... searchFields) {
			this.searchFields = toCSV(searchFields);
			return this;
		}

		public SuggestQuery setTop(int top) {
			this.top = top;
			return this;
		}

		public SuggestQuery setFilter(String filter) {
			this.filter = filter;
			return this;
		}

		public SuggestQuery setOrderBy(String... fields) {
			this.orderBy = toCSV(fields);
			return this;
		}

		public SuggestQuery setSelect(String... fields) {
			this.select = toCSV(fields);
			return this;
		}

	}

	public JsonObject suggestDocuments(String indexName, SuggestQuery query) throws WebApplicationException {
		WebTarget lookup = endpoint.path("indexes").path(indexName).path("docs/suggest");
		if (query.search != null) {
			lookup = lookup.queryParam("search", query.search);
		}
		if (query.fuzzy != null) {
			lookup = lookup.queryParam("fuzzy", query.fuzzy);
		}
		if (query.searchFields != null) {
			lookup = lookup.queryParam("searchFields", query.searchFields);
		}
		if (query.top > -1) {
			lookup = lookup.queryParam("$top", query.top);
		}
		if (query.filter != null) {
			lookup = lookup.queryParam("$filter", query.filter);
		}
		if (query.orderBy != null) {
			lookup = lookup.queryParam("$orderby", query.orderBy);
		}
		if (query.select != null) {
			lookup = lookup.queryParam("$select", query.select);
		}

		lookup = lookup.queryParam("api-version", AZURE_SEARCH_VERSION);

		return operation(lookup, "GET", Response.Status.OK.getStatusCode(), null);
	}

	public JsonObject operation(WebTarget target, String method, int expectedStatus, Entity<?> body) throws WebApplicationException {

		Builder builder = target.request();
		builder.header("api-key", masterKey);

		Response response = builder.method(method, body);

		if (response.getStatusInfo().getStatusCode() != expectedStatus) {
			throw new WebApplicationException(response);
		}

		if (response.hasEntity()) {
			return response.readEntity(JsonObject.class);
		}
		return null;

	}

	public static String toCSV(String[] fields) {
		if (fields.length > 0) {
			StringBuilder sb = new StringBuilder(fields.length);
			for (String s : fields) {
				sb.append(",").append(s);
			}
			return sb.substring(1);
		}
		return null;
	}

}
