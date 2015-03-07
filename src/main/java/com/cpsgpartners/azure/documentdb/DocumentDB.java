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
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilderException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

//import org.glassfish.jersey.filter.LoggingFilter;

//https://github.com/Azure/azure-documentdb-java
public class DocumentDB {

	public static final String AZURE_DOCUMENTDB_ENDPOINT = "https://%s.documents.azure.com";

	//DateTimeFormatter.RFC_1123_DATE_TIME does not pad the date as required by Azure
	public static final DateTimeFormatter RFC1123_DATE_TIME = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z");

	public static Pattern DOCUMENTDB_URI_PATTERN = Pattern.compile("dbs\\/([^\\/]+)\\/(colls\\/([^\\/]+)\\/)?(docs\\/([^\\/]+)\\/)?");

	public static enum ConsistencyLevel {
		Strong, Bounded, Session, Eventual;
	}

	public static enum IndexDirective {
		Include, Exclude;
	}

	public static class Id {
		private final String uri;
		private final String dbId;
		private final String collId;
		private final String docId;

		Id(String uri, String dbId, String collId, String docId) {
			this.uri = uri;
			this.dbId = dbId;
			this.collId = collId;
			this.docId = docId;
		}

		public String getURI() {
			return uri;
		}

		public String getDbId() {
			return dbId;
		}

		public String getCollId() {
			return collId;
		}

		public String getDocId() {
			return docId;
		}

	}

	//private final String id;
	private final String masterKey;
	private final Map<String, String> sessionStore = new WeakHashMap<String, String>();
	private final ConsistencyLevel consistencyLevel;
	private final Client client;
	private final WebTarget endpoint;

	public DocumentDB(String id, String masterKey, ConsistencyLevel consistencyLevel, Class<?>... components) {
		//this.id = id;
		this.masterKey = masterKey;
		this.consistencyLevel = consistencyLevel;
		client = ClientBuilder.newClient();
		if (components != null) {
			for (Class<?> c : components) {
				client.register(c);
			}
		}
		//client.register(new LoggingFilter());
		endpoint = client.target(String.format(AZURE_DOCUMENTDB_ENDPOINT, id));
	}

	public static Id parse(String idURI) throws UriBuilderException {
		Matcher m = DOCUMENTDB_URI_PATTERN.matcher(idURI);
		if (m.matches()) {
			return new Id(idURI, m.group(1) != null ? m.group(1) : "", m.group(3) != null ? m.group(3) : "", m.group(5) != null ? m.group(5) : "");
		}
		throw new UriBuilderException(String.format("invalid format %s", idURI));
	}

	public JsonObject createDatabase(String databaseId) throws WebApplicationException {
		return operation(endpoint.path("/dbs"), null, null, "POST", Response.Status.CREATED, Entity.entity(Json.createObjectBuilder().add("id", databaseId).build(), MediaType.APPLICATION_JSON_TYPE),
				JsonObject.class, "dbs", "");
	}

	public JsonObject getDatabase(String dbResourceId) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s", dbResourceId)), null, null, "GET", Response.Status.OK, null, JsonObject.class, "dbs", dbResourceId);
	}

	public JsonObject listDatabases() throws WebApplicationException {
		return operation(endpoint.path("/dbs"), null, null, "GET", Response.Status.OK, null, JsonObject.class, "dbs", "");
	}

	public void deleteDatabase(String dbResourceId) throws WebApplicationException {
		operation(endpoint.path(String.format("/dbs/%s", dbResourceId)), null, null, "DELETE", Response.Status.NO_CONTENT, null, null, "dbs", dbResourceId);
	}

	public JsonObject createCollection(String dbResourceId, String collectionId) throws WebApplicationException {
		return createCollection(dbResourceId, collectionId, null);
	}

	public JsonObject createCollection(String dbResourceId, String collectionId, JsonObject indexingPolicy) throws WebApplicationException {
		JsonObjectBuilder builder = Json.createObjectBuilder().add("id", collectionId);
		if (indexingPolicy != null) {
			builder.add("IndexingPolicy", indexingPolicy);
		}
		return operation(endpoint.path(String.format("/dbs/%s/colls/", dbResourceId)), null, null, "POST", Response.Status.CREATED, Entity.entity(builder.build(), MediaType.APPLICATION_JSON_TYPE),
				JsonObject.class, "colls", dbResourceId);
	}

	public JsonObject listCollections(String dbResourceId) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/", dbResourceId)), null, null, "GET", Response.Status.OK, null, JsonObject.class, "colls", dbResourceId);
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

	public <S, R> R createDocument(String dbResourceId, String collectionResId, S document, Class<R> responseType, IndexDirective indexDirective) throws WebApplicationException {

		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/", dbResourceId, collectionResId)), new IndexRequestHelper(indexDirective), null, "POST", Response.Status.CREATED,
				Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), responseType, "docs", collectionResId);

	}

	public <R> R listDocuments(String dbResourceId, String collectionResId, Class<R> responseType) throws WebApplicationException {
		WebTarget target = endpoint.path(String.format("/dbs/%s/colls/%s/docs/", dbResourceId, collectionResId));
		return operation(target, null, null, "GET", Response.Status.OK, Entity.entity(null, MediaType.WILDCARD_TYPE), responseType, "docs", collectionResId);
	}

	public <S> S getDocument(String dbResourceId, String collectionResId, String documentResId, Class<S> responseType) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/", dbResourceId, collectionResId, documentResId)), null, null, "GET", Response.Status.OK,
				Entity.entity(null, MediaType.WILDCARD_TYPE), responseType, "docs", documentResId);
	}

	public <S, R> R replaceDocument(String dbResourceId, String collectionResId, String documentResId, S document, Class<R> responseType, IndexDirective indexDirective) throws WebApplicationException {

		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/", dbResourceId, collectionResId, documentResId)), new IndexRequestHelper(indexDirective), null, "PUT",
				Response.Status.OK, Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), responseType, "docs", documentResId);
	}

	public void deleteDocument(String dbResourceId, String collectionResId, String documentResId) throws WebApplicationException {
		operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/", dbResourceId, collectionResId, documentResId)), null, null, "DELETE", Response.Status.NO_CONTENT,
				Entity.entity(null, MediaType.WILDCARD_TYPE), JsonObject.class, "docs", documentResId);
	}

	public static class QueryRequest implements RequestHandler {
		int pageSize = -1;
		String continuationToken = null;

		public QueryRequest(int pageSize, String continuationToken) {
			this.pageSize = pageSize;
			this.continuationToken = continuationToken;
		}

		@Override
		public void handle(Builder builder) {
			builder.header("x-ms-documentdb-isquery", "true");
			if (pageSize > 0) {
				builder.header("x-ms-max-item-count", pageSize);
			}
			if (continuationToken != null && !continuationToken.isEmpty()) {
				builder.header("x-ms-continuation", continuationToken);
			}

		}

	}

	public static class QueryResponse implements ResponseHandler {
		String continuationToken = null;

		@Override
		public void handle(Response response) {
			continuationToken = response.getHeaderString("x-ms-continuation");
		}

		public String getContinuationToken() {
			return continuationToken;
		}

	}

	public static interface QueryResult<R> {

		public List<R> getDocuments();

		public void setDocuments(List<R> documents);

		public String getRId();

		public void setRid(String rId);

		public String getContinuation();

		public void setContinuation(String continuation);

	}

	public <R> R queryDocuments(String dbResourceId, String collectionResId, String query, Class<R> responseType, int pageSize, String continuationToken) throws WebApplicationException {
		QueryResponse qresponse = new QueryResponse();
		R result = operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/", dbResourceId, collectionResId)), new QueryRequest(pageSize, continuationToken), qresponse, "POST",
				Response.Status.OK, Entity.entity(query, "application/sql"), responseType, "docs", collectionResId);
		if (result instanceof QueryResult) {
			QueryResult qResult = (QueryResult) result;
			qResult.setContinuation(qresponse.getContinuationToken());
		}
		return result;
	}

	/*public QueryResult queryDocuments(String dbResourceId, String collectionResId, String query, Map<String, String> parameters, int pageSize, String continuationToken) throws WebApplicationException {
		QueryResponse qresponse = new QueryResponse();
		JsonArrayBuilder paramBuilder = Json.createArrayBuilder();
		if (parameters != null) {
			for (Map.Entry<String, String> entry : parameters.entrySet()) {
				paramBuilder.add(Json.createObjectBuilder().add("name", entry.getKey()).add("value", entry.getValue()));
			}
		}
		JsonObject queryObject = Json.createObjectBuilder().add("query", query).add("parameters", paramBuilder).build();
		JsonObject result = operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/", dbResourceId, collectionResId)), new QueryRequest(pageSize, continuationToken), qresponse, "POST",
				Response.Status.OK, Entity.entity(queryObject, "application/query+json"), JsonObject.class, "docs", collectionResId);
		return new QueryResult(result, qresponse.getContinuationToken());
	}*/

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
				null, "POST", Response.Status.CREATED, Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "attachments", documentResId);

	}

	public JsonObject listAttachments(String dbResourceId, String collectionResId, String documentResId) throws WebApplicationException {
		WebTarget target = endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/", dbResourceId, collectionResId, documentResId));
		return operation(target, null, null, "GET", Response.Status.OK, Entity.entity(null, MediaType.WILDCARD_TYPE), JsonObject.class, "attachments", documentResId);
	}

	public JsonObject getAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/%s/", dbResourceId, collectionResId, documentResId, attachmentResId)), null, null, "GET",
				Response.Status.OK, Entity.entity(null, MediaType.WILDCARD_TYPE), JsonObject.class, "attachments", attachmentResId);
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
				fileName, indexDirective), null, "PUT", Response.Status.OK, Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "attachments", attachmentResId);
	}

	public JsonObject deleteAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/%s/", dbResourceId, collectionResId, documentResId, attachmentResId)), null, null, "DELETE",
				Response.Status.NO_CONTENT, Entity.entity(null, MediaType.WILDCARD_TYPE), JsonObject.class, "attachments", attachmentResId);
	}

	public <S, R> R operation(WebTarget target, RequestHandler reqHandler, ResponseHandler resHandler, String method, Response.Status expectedStatus, Entity<S> body, Class<R> responseType,
			String resourceType, String resourceId) throws WebApplicationException {

		String path = target.getUri().getPath();
		Builder builder = target.request();
		setHeaders(builder, path, method, resourceType, resourceId);
		if (reqHandler != null) {
			reqHandler.handle(builder);
		}
		Response response = builder.method(method, body);

		if (expectedStatus != null && response.getStatusInfo().getStatusCode() != expectedStatus.getStatusCode()) {
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

		if (responseType != null) {
			Class<R> responseTypeClass = (Class<R>) responseType;
			if (responseTypeClass.isAssignableFrom(Response.class)) {
				return (R) response;
			}
			if (response.hasEntity()) {
				R entity = response.readEntity(responseTypeClass);
				response.close();
				return entity;
			}

		}
		response.close();
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
