package com.cpsgpartners.azure.documentdb;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilderException;

//import org.glassfish.jersey.filter.LoggingFilter;

//https://github.com/Azure/azure-documentdb-java
public class DocumentDB {

	public static final String AZURE_DOCUMENTDB_ENDPOINT = "https://%s.documents.azure.com";
	public static final String AZURE_DOCUMENTDB_VERSION = "2015-04-08";

	//DateTimeFormatter.RFC_1123_DATE_TIME does not pad the date as required by Azure
	public static final DateTimeFormatter RFC1123_DATE_TIME = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z");

	public static Pattern DOCUMENTDB_URI_PATTERN = Pattern.compile("dbs\\/([^\\/]+)\\/(colls\\/([^\\/]+)\\/)?(([^\\/]+)\\/([^\\/]+)\\/)?(attachments\\/([^\\/]+)\\/?)?");

	public static enum ConsistencyLevel {
		Strong, Bounded, Session, Eventual;
	}

	public static enum IndexDirective {
		Include, Exclude;
	}

	public static enum ETagMode {
		MATCH, NO_MATCH
	}

	public static class ETag {

		private final ETagMode mode;
		private final String value;

		public ETag(ETagMode mode, String value) {
			this.mode = mode;
			this.value = value;
		}

		public ETagMode getMode() {
			return mode;
		}

		public String getValue() {
			return value;
		}

	}

	public static class Id {
		private final String uri;
		private final String dbId;
		private final String collId;
		private final String resType;
		private final String resId;
		private final String attId;

		Id(String dbId, String collId, String resType, String resId) {
			this(dbId, collId, resType, resId, null);
		}

		Id(String dbId, String collId, String resType, String resId, String attId) {
			this.dbId = dbId;
			this.collId = collId;
			this.resType = resType;
			this.resId = resId;
			this.attId=attId;
			
			StringBuilder sb = new StringBuilder();
			if (dbId != null && !dbId.isEmpty()) {
				sb.append("dbs").append("/").append(dbId).append("/");
			}
			if (collId != null && !collId.isEmpty()) {
				sb.append("colls").append("/").append(collId).append("/");
			}
			if (resType != null && !resType.isEmpty() && resId != null && !resId.isEmpty()) {
				sb.append(resType).append("/").append(resId).append("/");
			}
			if (attId != null && !attId.isEmpty()) {
				sb.append("attachments").append("/").append(attId).append("/");
			}
			this.uri = sb.toString();
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

		public String getResType() {
			return resType;
		}

		public String getResId() {
			return resId;
		}

		public String getAttId() {
			return attId;
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
			return new Id(m.group(1) != null ? m.group(1) : "", m.group(3) != null ? m.group(3) : "", m.group(5) != null ? m.group(5) : "", m.group(6) != null ? m.group(6) : "", m.group(8) != null ? m.group(8) : "");
		}
		throw new UriBuilderException(String.format("invalid ID format %s", idURI));
	}

	public static Id newDocumentId(Id collectionId, String documentId) throws UriBuilderException {
		return new Id(collectionId.getDbId(), collectionId.getCollId(), "docs", documentId);
	}
	
	

	public JsonObject createDatabase(String databaseId) throws WebApplicationException {
		return operation(endpoint.path("/dbs"), null, null, "POST", null, Response.Status.CREATED, Entity.entity(Json.createObjectBuilder().add("id", databaseId).build(), MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "dbs", "");
	}

	public JsonObject getDatabase(String dbResourceId) throws WebApplicationException {
		return getDatabase(dbResourceId, null);
	}

	public JsonObject getDatabase(String dbResourceId, ETag etag) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s", dbResourceId)), null, null, "GET", etag, Response.Status.OK, null, JsonObject.class, "dbs", dbResourceId);
	}

	public JsonObject listDatabases() throws WebApplicationException {
		return operation(endpoint.path("/dbs"), null, null, "GET", null, Response.Status.OK, null, JsonObject.class, "dbs", "");
	}

	public void deleteDatabase(String dbResourceId) throws WebApplicationException {
		deleteDatabase(dbResourceId, null);
	}

	public void deleteDatabase(String dbResourceId, ETag etag) throws WebApplicationException {
		operation(endpoint.path(String.format("/dbs/%s", dbResourceId)), null, null, "DELETE", etag, Response.Status.NO_CONTENT, null, null, "dbs", dbResourceId);
	}

	public JsonObject createCollection(String dbResourceId, String collectionId) throws WebApplicationException {
		return createCollection(dbResourceId, collectionId, null);
	}

	public JsonObject createCollection(String dbResourceId, String collectionId, JsonObject indexingPolicy) throws WebApplicationException {
		JsonObjectBuilder builder = Json.createObjectBuilder().add("id", collectionId);
		if (indexingPolicy != null) {
			builder.add("IndexingPolicy", indexingPolicy);
		}
		return operation(endpoint.path(String.format("/dbs/%s/colls/", dbResourceId)), null, null, "POST", null, Response.Status.CREATED, Entity.entity(builder.build(), MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "colls", dbResourceId);
	}

	public JsonObject listCollections(String dbResourceId) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/", dbResourceId)), null, null, "GET", null, Response.Status.OK, null, JsonObject.class, "colls", dbResourceId);
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

		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/", dbResourceId, collectionResId)), new IndexRequestHelper(indexDirective), null, "POST", null, Response.Status.CREATED, Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), responseType, "docs", collectionResId);

	}

	public <R> R listDocuments(String dbResourceId, String collectionResId, Class<R> responseType) throws WebApplicationException {
		WebTarget target = endpoint.path(String.format("/dbs/%s/colls/%s/docs/", dbResourceId, collectionResId));
		return operation(target, null, null, "GET", null, Response.Status.OK, null, responseType, "docs", collectionResId);
	}

	public <S> S getDocument(String dbResourceId, String collectionResId, String documentResId, Class<S> responseType) throws WebApplicationException {
		return getDocument(dbResourceId, collectionResId, documentResId, null, responseType);
	}

	public <S> S getDocument(String dbResourceId, String collectionResId, String documentResId, ETag etag, Class<S> responseType) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/", dbResourceId, collectionResId, documentResId)), null, null, "GET", etag, Response.Status.OK, null, responseType, "docs", documentResId);
	}

	public <S, R> R replaceDocument(String dbResourceId, String collectionResId, String documentResId, S document, Class<R> responseType, IndexDirective indexDirective) throws WebApplicationException {
		return replaceDocument(dbResourceId, collectionResId, documentResId, null, document, responseType, indexDirective);
	}

	public <S, R> R replaceDocument(String dbResourceId, String collectionResId, String documentResId, ETag etag, S document, Class<R> responseType, IndexDirective indexDirective) throws WebApplicationException {

		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/", dbResourceId, collectionResId, documentResId)), new IndexRequestHelper(indexDirective), null, "PUT", etag, Response.Status.OK, Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), responseType, "docs", documentResId);
	}

	public void deleteDocument(String dbResourceId, String collectionResId, String documentResId) throws WebApplicationException {
		deleteDocument(dbResourceId, collectionResId, documentResId, null);
	}

	public void deleteDocument(String dbResourceId, String collectionResId, String documentResId, ETag etag) throws WebApplicationException {
		operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/", dbResourceId, collectionResId, documentResId)), null, null, "DELETE", etag, Response.Status.NO_CONTENT, null, JsonObject.class, "docs", documentResId);
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

		public void setRId(String rId);

		public String getContinuation();

		public void setContinuation(String continuation);

	}

	public <R> R queryDocuments(String dbResourceId, String collectionResId, String query, Map<String, String> parameters, Object responseType, int pageSize, String continuationToken) throws WebApplicationException {
		QueryResponse qresponse = new QueryResponse();
		JsonArrayBuilder paramBuilder = Json.createArrayBuilder();
		if (parameters != null) {
			for (Map.Entry<String, String> entry : parameters.entrySet()) {
				paramBuilder.add(Json.createObjectBuilder().add("name", entry.getKey()).add("value", entry.getValue()));
			}
		}
		JsonObject queryObject = Json.createObjectBuilder().add("query", query).add("parameters", paramBuilder).build();
		R result = operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/", dbResourceId, collectionResId)), new QueryRequest(pageSize, continuationToken), qresponse, "POST", null, Response.Status.OK, Entity.entity(queryObject, "application/query+json"), responseType, "docs", collectionResId);
		if (result instanceof QueryResult) {
			QueryResult qResult = (QueryResult) result;
			qResult.setContinuation(qresponse.getContinuationToken());
		}
		return result;
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

	public JsonObject createAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentId, String fileName, String contentType, String media, JsonObject extraData) throws WebApplicationException {
		return createAttachment(dbResourceId, collectionResId, documentResId, attachmentId, fileName, contentType, media, extraData, null);
	}

	public JsonObject createAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentId, String fileName, String contentType, String media, JsonObject extraData, IndexDirective indexDirective) throws WebApplicationException {
		if (attachmentId == null || contentType == null || media == null) {
			throw new WebApplicationException(String.format("required attachment value missing: id: %s contentType: %s media: %s", attachmentId, contentType, media));
		}
		JsonObjectBuilder documentBuilder = Json.createObjectBuilder().add("id", attachmentId).add("contentType", contentType).add("media", media);
		if (extraData != null) {
			for (Entry<String, JsonValue> entry : extraData.entrySet()) {
				documentBuilder.add(entry.getKey(), entry.getValue());
			}
		}
		JsonObject document = documentBuilder.build();
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/", dbResourceId, collectionResId, documentResId)), new FileNameIndexRequestHelper(fileName, indexDirective), null, "POST", null, Response.Status.CREATED, Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "attachments", documentResId);

	}

	public JsonObject listAttachments(String dbResourceId, String collectionResId, String documentResId) throws WebApplicationException {
		WebTarget target = endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/", dbResourceId, collectionResId, documentResId));
		return operation(target, null, null, "GET", null, Response.Status.OK, null, JsonObject.class, "attachments", documentResId);
	}

	public JsonObject getAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId) throws WebApplicationException {
		return getAttachment(dbResourceId, collectionResId, documentResId, attachmentResId, null);
	}

	public JsonObject getAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId, ETag etag) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/%s/", dbResourceId, collectionResId, documentResId, attachmentResId)), null, null, "GET", etag, Response.Status.OK, null, JsonObject.class, "attachments", attachmentResId);
	}

	public JsonObject replaceAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId, String attachmentId, String fileName, String contentType, String media, JsonObject extraData) throws WebApplicationException {
		return replaceAttachment(dbResourceId, collectionResId, documentResId, attachmentResId, null, attachmentId, fileName, contentType, media, extraData, null);
	}

	public JsonObject replaceAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId, ETag etag, String attachmentId, String fileName, String contentType, String media, JsonObject extraData) throws WebApplicationException {
		return replaceAttachment(dbResourceId, collectionResId, documentResId, attachmentResId, etag, attachmentId, fileName, contentType, media, extraData, null);

	}

	public JsonObject replaceAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId, ETag etag, String attachmentId, String fileName, String contentType, String media, JsonObject extraData, IndexDirective indexDirective) throws WebApplicationException {
		if (attachmentId == null || contentType == null || media == null) {
			throw new WebApplicationException(String.format("required attachment value missing: id: %s contentType: %s media: %s", attachmentId, contentType, media));
		}
		JsonObjectBuilder documentBuilder = Json.createObjectBuilder().add("id", attachmentId).add("contentType", contentType).add("media", media);
		if (extraData != null) {
			for (Entry<String, JsonValue> entry : extraData.entrySet()) {
				documentBuilder.add(entry.getKey(), entry.getValue());
			}
		}
		JsonObject document = documentBuilder.build();
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/%s/", dbResourceId, collectionResId, documentResId, attachmentResId)), new FileNameIndexRequestHelper(fileName, indexDirective), null, "PUT", etag, Response.Status.OK, Entity.entity(document, MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "attachments",
				attachmentResId);
	}

	public JsonObject deleteAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId) throws WebApplicationException {
		return deleteAttachment(dbResourceId, collectionResId, documentResId, attachmentResId, null);
	}

	public JsonObject deleteAttachment(String dbResourceId, String collectionResId, String documentResId, String attachmentResId, ETag etag) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/docs/%s/attachments/%s/", dbResourceId, collectionResId, documentResId, attachmentResId)), null, null, "DELETE", etag, Response.Status.NO_CONTENT, null, JsonObject.class, "attachments", attachmentResId);
	}

	public JsonObject createStoredProcedure(String dbResourceId, String collectionResId, String spName, String spBody) throws WebApplicationException {
		JsonObject storedProcedure = Json.createObjectBuilder().add("id", spName).add("body", spBody).build();
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/sprocs/", dbResourceId, collectionResId)), null, null, "POST", null, Response.Status.CREATED, Entity.entity(storedProcedure, MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "sprocs", collectionResId);
	}

	public JsonObject replaceStoredProcedure(String dbResourceId, String collectionResId, String spResId, String spName, String spBody) throws WebApplicationException {
		JsonObject storedProcedure = Json.createObjectBuilder().add("id", spName).add("body", spBody).build();
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/sprocs/%s/", dbResourceId, collectionResId, spResId)), null, null, "PUT", null, Response.Status.OK, Entity.entity(storedProcedure, MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "sprocs", spResId);
	}

	public JsonObject listStoredProcedures(String dbResourceId, String collectionResId) throws WebApplicationException {
		WebTarget target = endpoint.path(String.format("/dbs/%s/colls/%s/sprocs/", dbResourceId, collectionResId));
		return operation(target, null, null, "GET", null, Response.Status.OK, null, JsonObject.class, "sprocs", collectionResId);
	}

	//TODO can also query sprocs i.e.  "select * from root r where r.id = \"sproc name\"

	public <S, R> R executeStoredProcedure(String dbResourceId, String collectionResId, String spResId, S document, Object responseType) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/sprocs/%s/", dbResourceId, collectionResId, spResId)), null, null, "POST", null, Response.Status.OK, document != null ? Entity.entity(document, MediaType.APPLICATION_JSON_TYPE) : null, responseType, "sprocs", spResId);
	}

	public void deleteStoredProcedure(String dbResourceId, String collectionResId, String spResId) throws WebApplicationException {
		operation(endpoint.path(String.format("/dbs/%s/colls/%s/sprocs/%s/", dbResourceId, collectionResId, spResId)), null, null, "DELETE", null, Response.Status.NO_CONTENT, null, null, "sprocs", spResId);
	}

	public JsonObject createTrigger(String dbResourceId, String collectionResId, String triggerName, String triggerBody) throws WebApplicationException {
		JsonObject storedProcedure = Json.createObjectBuilder().add("id", triggerName).add("body", triggerBody).build();
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/triggers/", dbResourceId, collectionResId)), null, null, "POST", null, Response.Status.CREATED, Entity.entity(storedProcedure, MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "triggers", collectionResId);
	}

	public JsonObject replaceTrigger(String dbResourceId, String collectionResId, String triggerResId, String triggerName, String triggerBody) throws WebApplicationException {
		JsonObject storedProcedure = Json.createObjectBuilder().add("id", triggerName).add("body", triggerBody).build();
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/triggers/%s/", dbResourceId, collectionResId, triggerResId)), null, null, "PUT", null, Response.Status.OK, Entity.entity(storedProcedure, MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "triggers", triggerResId);
	}

	public JsonObject listTrigger(String dbResourceId, String collectionResId) throws WebApplicationException {
		WebTarget target = endpoint.path(String.format("/dbs/%s/colls/%s/triggers/", dbResourceId, collectionResId));
		return operation(target, null, null, "GET", null, Response.Status.OK, null, JsonObject.class, "triggers", collectionResId);
	}

	//TODO can also query sprocs i.e.  "select * from root r where r.id = \"sproc name\"

	public <S, R> R executeTrigger(String dbResourceId, String collectionResId, String triggerResId, S document, Class<R> responseType) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/triggers/%s/", dbResourceId, collectionResId, triggerResId)), null, null, "POST", null, Response.Status.OK, document != null ? Entity.entity(document, MediaType.APPLICATION_JSON_TYPE) : null, responseType, "triggers", triggerResId);
	}

	public void deleteTrigger(String dbResourceId, String collectionResId, String triggerResId) throws WebApplicationException {
		operation(endpoint.path(String.format("/dbs/%s/colls/%s/triggers/%s/", dbResourceId, collectionResId, triggerResId)), null, null, "DELETE", null, Response.Status.NO_CONTENT, null, null, "triggers", triggerResId);
	}

	public JsonObject createUDF(String dbResourceId, String collectionResId, String udfName, String udfBody) throws WebApplicationException {
		JsonObject storedProcedure = Json.createObjectBuilder().add("id", udfName).add("body", udfBody).build();
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/udfs/", dbResourceId, collectionResId)), null, null, "POST", null, Response.Status.CREATED, Entity.entity(storedProcedure, MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "udfs", collectionResId);
	}

	public JsonObject replaceUDF(String dbResourceId, String collectionResId, String udfResId, String udfName, String udfBody) throws WebApplicationException {
		JsonObject storedProcedure = Json.createObjectBuilder().add("id", udfName).add("body", udfBody).build();
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/udfs/%s/", dbResourceId, collectionResId, udfResId)), null, null, "PUT", null, Response.Status.OK, Entity.entity(storedProcedure, MediaType.APPLICATION_JSON_TYPE), JsonObject.class, "udfs", udfResId);
	}

	public JsonObject listUDFs(String dbResourceId, String collectionResId) throws WebApplicationException {
		WebTarget target = endpoint.path(String.format("/dbs/%s/colls/%s/udfs/", dbResourceId, collectionResId));
		return operation(target, null, null, "GET", null, Response.Status.OK, null, JsonObject.class, "udfs", collectionResId);
	}

	//TODO can also query sprocs i.e.  "select * from root r where r.id = \"sproc name\"

	public <S, R> R executeUDF(String dbResourceId, String collectionResId, String udfResId, S document, Class<R> responseType) throws WebApplicationException {
		return operation(endpoint.path(String.format("/dbs/%s/colls/%s/udfs/%s/", dbResourceId, collectionResId, udfResId)), null, null, "POST", null, Response.Status.OK, document != null ? Entity.entity(document, MediaType.APPLICATION_JSON_TYPE) : null, responseType, "udfs", udfResId);
	}

	public void deleteUDF(String dbResourceId, String collectionResId, String udfResId) throws WebApplicationException {
		operation(endpoint.path(String.format("/dbs/%s/colls/%s/udfs/%s/", dbResourceId, collectionResId, udfResId)), null, null, "DELETE", null, Response.Status.NO_CONTENT, null, null, "udfs", udfResId);
	}

	public <S, R> R operation(WebTarget target, RequestHandler reqHandler, ResponseHandler resHandler, String method, ETag etag, Response.Status expectedStatus, Entity<S> body, Object responseType, String resourceType, String resourceId) throws WebApplicationException {

		String path = target.getUri().getPath();
		Builder builder = target.request();
		setHeaders(builder, path, method, resourceType, resourceId, etag);
		if (reqHandler != null) {
			reqHandler.handle(builder);
		}
		Response response = builder.method(method, body);

		if (expectedStatus != null && response.getStatusInfo().getStatusCode() != expectedStatus.getStatusCode() && !(etag != null && response.getStatusInfo().getStatusCode() != Response.Status.NOT_MODIFIED.getStatusCode())) {
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
			if (responseType instanceof Class) {
				Class<R> responseTypeClass = (Class<R>) responseType;
				if (responseTypeClass.isAssignableFrom(Response.class)) {
					return (R) response;
				}
				if (response.hasEntity()) {
					R entity = response.readEntity(responseTypeClass);
					response.close();
					return entity;
				}
			} else if (responseType instanceof GenericType && response.hasEntity()) {
				R entity = response.readEntity((GenericType<R>) responseType);
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

	public Builder setHeaders(Builder builder, String path, String verb, String resourceType, String resourceId, ETag etag) throws WebApplicationException {
		try {
			ZonedDateTime currentTime = ZonedDateTime.now(ZoneId.of("GMT"));
			String date = RFC1123_DATE_TIME.format(currentTime);
			builder.header("x-ms-date", date);
			builder.header("x-ms-version", AZURE_DOCUMENTDB_VERSION);
			builder.accept(MediaType.APPLICATION_JSON_TYPE);
			if (etag != null) {
				switch (etag.getMode()) {
				case MATCH:
					builder.header(HttpHeaders.IF_MATCH, etag.value);
					break;
				case NO_MATCH:
					builder.header(HttpHeaders.IF_NONE_MATCH, etag.value);
					break;
				}
			}
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

}
