package com.cpsgpartners.azure.documentdb;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

//import org.glassfish.jersey.filter.LoggingFilter;

//https://github.com/Azure/azure-documentdb-java
public class DocumentDB {

	public static final String AZURE_DOCUMENTDB_ENDPOINT = "https://%s.documents.azure.com";
	
	//DateTimeFormatter.RFC_1123_DATE_TIME does not pad the date as required by Azure
	public static final DateTimeFormatter RFC1123_DATE_TIME = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z");


	public static enum ConsistencyLevel {
		Strong, Bounded, Session, Eventual;
	}

	public static enum IndexDirective {
		Include, Exclude;
	}

	private final String masterKey;
	private final Map<String, String> sessionStore = new WeakHashMap<String, String>();
	private final ConsistencyLevel consistencyLevel;
	private final Client client;
	private final WebTarget endpoint;

	public DocumentDB(String id, String masterKey, ConsistencyLevel consistencyLevel) {
		this.masterKey = masterKey;
		this.consistencyLevel = consistencyLevel;
		client = ClientBuilder.newClient();
		//client.register(new LoggingFilter());
		endpoint = client.target(String.format(AZURE_DOCUMENTDB_ENDPOINT, id));
	}

	public JsonObject createDatabase(String databaseId) throws WebApplicationException {
		return operation(endpoint.path("/dbs"), null, null, "POST", Response.Status.CREATED.getStatusCode(),
				Entity.entity(Json.createObjectBuilder().add("id", databaseId).build(), MediaType.APPLICATION_JSON_TYPE), "dbs", "");
	}

	public JsonObject listDatabases() throws WebApplicationException {
		return operation(endpoint.path("/dbs"), null, null, "GET", Response.Status.OK.getStatusCode(), null, "dbs", "");
	}

	public void deleteDatabase(String dbResourceId) throws WebApplicationException {
		operation(endpoint.path(String.format("/dbs/%s", dbResourceId)), null, null, "DELETE", Response.Status.NO_CONTENT.getStatusCode(), null, "dbs", dbResourceId);
	}

	public JsonObject createCollection(String dbResourceId, String collectionId) throws WebApplicationException {
		return createCollection(dbResourceId, collectionId, null);
	}

	public JsonObject createCollection(String dbResourceId, String collectionId, JsonObject indexingPolicy) throws WebApplicationException {
		JsonObjectBuilder builder = Json.createObjectBuilder().add("id", collectionId);
		if (indexingPolicy != null) {
			builder.add("IndexingPolicy", indexingPolicy);
		}
		return operation(endpoint.path(String.format("/dbs/%s/colls/", dbResourceId)), null, null, "POST", Response.Status.CREATED.getStatusCode(),
				Entity.entity(builder.build(), MediaType.APPLICATION_JSON_TYPE), "colls", dbResourceId);
	}

	public JsonObject listCollections(String dbResourceId) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/", dbResourceId)), null, null, "GET", Response.Status.OK.getStatusCode(), null, "colls", dbResourceId);
	}

	public JsonObject createDocument(String dbResourceId, String collectionResId, JsonObject document) throws WebApplicationException {
		return createDocument(dbResourceId, collectionResId, document, null);
	}

	public static class IndexRequestHelper implements RequestHandler {
		IndexDirective indexDirective;

		public IndexRequestHelper(IndexDirective indexDirective) {
			this.indexDirective = indexDirective;
		}

		@Override
		public void handle(Builder builder) {
			if (indexDirective != null) {
				builder.header("x-ms-indexing-directive", indexDirective.name());
			}

		}

	}

	public JsonObject createDocument(String dbResourceId, String collectionResId, JsonObject document, IndexDirective indexDirective) throws WebApplicationException {
		if (document.getString("id") == null) {
			throw new WebApplicationException("id is required attribute of document");
		}

		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/", dbResourceId, collectionResId)), new IndexRequestHelper(indexDirective), null, "POST",
				Response.Status.CREATED.getStatusCode(), Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), "docs", collectionResId);

	}

	public JsonObject listDocuments(String dbResourceId, String collectionResId) throws WebApplicationException {
		WebTarget target = endpoint.path(String.format("/dbs/%s/colls/%s/docs/", dbResourceId, collectionResId));
		return operation(target, null, null, "GET", Response.Status.OK.getStatusCode(), Entity.entity(null, MediaType.WILDCARD_TYPE), "docs", collectionResId);
	}

	public JsonObject getDocument(String dbResourceId, String collectionResId, String documentResId) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/", dbResourceId, collectionResId, documentResId)), null, null, "GET", Response.Status.OK.getStatusCode(),
				Entity.entity(null, MediaType.WILDCARD_TYPE), "docs", documentResId);
	}

	public JsonObject replaceDocument(String dbResourceId, String collectionResId, String documentResId, JsonObject document) throws WebApplicationException {
		return replaceDocument(dbResourceId, collectionResId, documentResId, document, null);
	}

	public JsonObject replaceDocument(String dbResourceId, String collectionResId, String documentResId, JsonObject document, IndexDirective indexDirective) throws WebApplicationException {

		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/", dbResourceId, collectionResId, documentResId)), new IndexRequestHelper(indexDirective), null, "PUT",
				Response.Status.OK.getStatusCode(), Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), "docs", documentResId);
	}

	public JsonObject deleteDocument(String dbResourceId, String collectionResId, String documentResId) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/", dbResourceId, collectionResId, documentResId)), null, null, "DELETE", Response.Status.NO_CONTENT.getStatusCode(),
				Entity.entity(null, MediaType.WILDCARD_TYPE), "docs", documentResId);
	}

	public static class QueryResult implements Iterable<JsonObject> {
		private final JsonObject queryResult;
		private final String continuation;

		QueryResult(JsonObject queryResult, String continuation) {
			this.queryResult = queryResult;
			this.continuation = continuation;
		}

		public String getContinuation() {
			return continuation;
		}

		@Override
		public Iterator<JsonObject> iterator() {
			return new Iterator<JsonObject>() {
				int counter = 0;
				JsonArray docArray = queryResult.getJsonArray("Documents");

				@Override
				public boolean hasNext() {
					return counter < docArray.size();
				}

				@Override
				public JsonObject next() {
					return docArray.getJsonObject(counter++);
				}

			};
		}

		@Override
		public String toString() {
			return "QueryResult [queryResult=" + queryResult + ", continuation=" + continuation + "]";
		}

	}

	public QueryResult queryDocuments(String dbResourceId, String collectionResId, String query, int pageSize, String continuationToken) throws WebApplicationException {
		RequestHandler reqHelper = (b) -> {
			b.header("x-ms-documentdb-isquery", "true");
			if (pageSize > 0) {
				b.header("x-ms-max-item-count", pageSize);
			}
			if (continuationToken != null && !continuationToken.isEmpty()) {
				b.header("x-ms-continuation", continuationToken);
			}
		};
		String[] newToken = new String[1];
		ResponseHandler resHelper = (r) -> {
			newToken[0] = r.getHeaderString("x-ms-continuation");
		};

		JsonObject result = operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/", dbResourceId, collectionResId)), reqHelper, resHelper, "POST", Response.Status.OK.getStatusCode(),
				Entity.entity(query, "application/sql"), "docs", collectionResId);
		return new QueryResult(result, newToken[0]);
	}

	public static class FileNameIndexRequestHelper implements RequestHandler {
		String fileName;
		IndexDirective indexDirective;

		public FileNameIndexRequestHelper(String fileName, IndexDirective indexDirective) {
			this.fileName = fileName;
			this.indexDirective = indexDirective;
		}

		@Override
		public void handle(Builder builder) {
			builder.header("Slug", fileName);
			if (indexDirective != null) {
				builder.header("x-ms-indexing-directive", indexDirective.name());
			}
		}

	}

	public JsonObject createAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentId, String fileName, String contentType, String media)
			throws WebApplicationException {
		return createAttachment(dbResourceId, collectionResId, documentResId, attachmentId, fileName, contentType, media, null);
	}

	public JsonObject createAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentId, String fileName, String contentType, String media,
			IndexDirective indexDirective) throws WebApplicationException {
		if (attachmentId == null || contentType == null || media == null) {
			throw new WebApplicationException(String.format("required attachment value missing: id: %s contentType: %s media: %s", attachmentId, contentType, media));
		}
		JsonObject document = Json.createObjectBuilder().add("id", attachmentId).add("contentType", contentType).add("media", media).build();
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/", dbResourceId, collectionResId, documentResId)), new FileNameIndexRequestHelper(fileName, indexDirective),
				null, "POST", Response.Status.CREATED.getStatusCode(), Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), "attachments", documentResId);

	}

	public JsonObject listAttachments(String dbResourceId, String collectionResId, String documentResId) throws WebApplicationException {
		WebTarget target = endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/", dbResourceId, collectionResId, documentResId));
		return operation(target, null, null, "GET", Response.Status.OK.getStatusCode(), Entity.entity(null, MediaType.WILDCARD_TYPE), "attachments", documentResId);
	}

	public JsonObject getAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/%s/", dbResourceId, collectionResId, documentResId, attachmentResId)), null, null, "GET",
				Response.Status.OK.getStatusCode(), Entity.entity(null, MediaType.WILDCARD_TYPE), "attachments", attachmentResId);
	}

	public JsonObject replaceAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId, String attachmentId, String fileName, String contentType,
			String media) throws WebApplicationException {
		return replaceAttachment(dbResourceId, collectionResId, documentResId, attachmentResId, attachmentId, fileName, contentType, media, null);

	}

	public JsonObject replaceAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId, String attachmentId, String fileName, String contentType,
			String media, IndexDirective indexDirective) throws WebApplicationException {
		if (attachmentId == null || contentType == null || media == null) {
			throw new WebApplicationException(String.format("required attachment value missing: id: %s contentType: %s media: %s", attachmentId, contentType, media));
		}
		JsonObject document = Json.createObjectBuilder().add("id", attachmentId).add("contentType", contentType).add("media", media).build();

		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/%s/", dbResourceId, collectionResId, documentResId, attachmentResId)), new FileNameIndexRequestHelper(
				fileName, indexDirective), null, "PUT", Response.Status.OK.getStatusCode(), Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), "attachments", attachmentResId);
	}

	public JsonObject deleteAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/%s/", dbResourceId, collectionResId, documentResId, attachmentResId)), null, null, "DELETE",
				Response.Status.NO_CONTENT.getStatusCode(), Entity.entity(null, MediaType.WILDCARD_TYPE), "attachments", attachmentResId);
	}

	public JsonObject operation(WebTarget target, RequestHandler reqHandler, ResponseHandler resHandler, String method, int expectedStatus, Entity<?> body, String resourceType, String resourceId)
			throws WebApplicationException {

		String path = target.getUri().getPath();
		Builder builder = target.request();
		setHeaders(builder, path, method, resourceType, resourceId);
		if (reqHandler != null) {
			reqHandler.handle(builder);
		}
		Response response = builder.method(method, body);

		if (response.getStatusInfo().getStatusCode() != expectedStatus) {
			throw new WebApplicationException(response);
		}

		if (resHandler != null) {
			resHandler.handle(response);
		}
		//session is set as cookie as well
		if (sessionStore != null && resourceId != null && !resourceId.isEmpty()) {
			//builder.
			String token = response.getHeaderString("x-ms-session-token");
			sessionStore.put(path, token);
		}

		if (response.hasEntity()) {
			return response.readEntity(JsonObject.class);
		}
		return null;

	}

	public static interface RequestHandler {

		public void handle(Builder builder);

	}

	public static interface ResponseHandler {

		public void handle(Response response);

	}

	public Builder setHeaders(Builder builder, String path, String verb, String resourceType, String resourceId) throws WebApplicationException {
		try {
			ZonedDateTime currentTime = ZonedDateTime.now(ZoneId.of("GMT"));
			String date = RFC1123_DATE_TIME.format(currentTime);
			builder.header("x-ms-date", date);
			builder.accept(MediaType.APPLICATION_JSON_TYPE);
			String stringToSign = String.format("%s\n%s\n%s\n%s\n%s\n", verb, resourceType, resourceId, date, "").toLowerCase();
			Mac sha256_HMAC = Mac.getInstance("HMACSHA256");
			SecretKeySpec secret_key = new SecretKeySpec(Base64.getDecoder().decode(masterKey), "HmacSHA256");
			sha256_HMAC.init(secret_key);
			String signature = Base64.getEncoder().encodeToString(sha256_HMAC.doFinal(stringToSign.getBytes()));
			builder.header("Authorization", URLEncoder.encode(String.format("type=master&ver=1.0&sig=%s", signature), "UTF-8"));

			if (consistencyLevel != null) {
				builder.header("x-ms-consistency-level", consistencyLevel.name());
				if (consistencyLevel == ConsistencyLevel.Session) {
					String token = sessionStore.get(path);
					if (token != null) {
						builder.header("x-ms-session-token", token);
					}
				}
			}
			return builder;
		} catch (NoSuchAlgorithmException | InvalidKeyException | UnsupportedEncodingException e) {
			throw new WebApplicationException(e);
		}
	}

	public Builder setPageOptions(Builder builder, int pageSize) {
		if (pageSize > -1) {
			builder.header("x-ms-max-item-count", pageSize);
		}
		//"x-ms-continuation";
		//"x-ms-max-item-count";
		return builder;
	}

}
