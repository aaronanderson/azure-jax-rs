package com.cpsgpartners.azure.storage;

import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

//import org.glassfish.jersey.filter.LoggingFilter;

//https://github.com/Azure/azure-storage-java
public class Storage {

	public static final String AZURE_STORAGE_ENDPOINT = "https://%s.blob.core.windows.net";
	public static final String AZURE_STORAGE_VERSION = "2014-02-14";
	public static final DateTimeFormatter ISO8061_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	public static final DateTimeFormatter ISO8061_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX");

	private final String id;
	private final String masterKey;
	private final Client client;
	private final WebTarget endpoint;

	public Storage(String id, String masterKey) {
		this.id = id;
		this.masterKey = masterKey;
		client = ClientBuilder.newClient(); //.property("jersey.config.client.suppressHttpComplianceValidation", "true");//otherwise container put without body causes error;
		//client.register(new LoggingFilter());
		endpoint = client.target(String.format(AZURE_STORAGE_ENDPOINT, id));
	}

	//leases currently not supported

	public void createContainer(String containerName) throws WebApplicationException {
		//Entity required so Content-Length 0 sent. Would like to send empty Content-Type value but it is not possible
		operation(endpoint.path(containerName).queryParam("restype", "container"), "PUT", null, Response.Status.CREATED.getStatusCode(), Entity.entity("", MediaType.WILDCARD_TYPE), null);
	}

	public void deleteContainer(String containerName) throws WebApplicationException {
		//Entity required so Content-Length 0 sent. Would like to send empty Content-Type value but it is not possible
		operation(endpoint.path(containerName).queryParam("restype", "container"), "DELETE", null, Response.Status.ACCEPTED.getStatusCode(), Entity.entity(null, MediaType.WILDCARD_TYPE), null);
	}

	public static class BlobFile implements Serializable {

		private static final long serialVersionUID = 1L;

		public static Pattern FILE_NAME = Pattern.compile(".*filename=\"(.*)\"");
		public final InputStream content;
		public final String fileName;
		public final String contentType;

		public BlobFile(InputStream content, String fileName, String contentType) {
			this.content = content;
			this.fileName = fileName;
			this.contentType = contentType;
		}

		public BlobFile(Response response) {
			this.content = response.readEntity(InputStream.class);
			String fileName = response.getHeaderString("Content-Disposition");
			Matcher m = FILE_NAME.matcher(fileName);
			if (m.matches()) {
				this.fileName = m.group(1);
			} else {
				this.fileName = null;
			}

			this.contentType = response.getMediaType().toString();
		}

	}

	//Block Blob, 64MB single upload maximum
	public void createOrUpdateBlob(String containerName, String blobName, BlobFile blob) throws WebApplicationException {
		MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
		headers.add("x-ms-blob-content-disposition", String.format("attachment; filename=\"%s\"", blob.fileName));
		headers.add("x-ms-blob-type", "BlockBlob");
		operation(endpoint.path(containerName).path(blobName), "PUT", headers, Response.Status.CREATED.getStatusCode(), Entity.entity(blob.content, blob.contentType), null);
	}

	//TODO possibly specify last modified header
	public BlobFile getBlob(String containerName, String blobName) throws WebApplicationException {
		return new BlobFile(operation(endpoint.path(containerName).path(blobName), "GET", null, Response.Status.OK.getStatusCode(), null, Response.class));
	}

	public String sharedAccessURL(String containerName, String blobName, String permissions, ZonedDateTime startTime, Duration expiryDuration, boolean longFormat) {

		String signedResource = "c";
		UriBuilder request = endpoint.path(containerName).getUriBuilder();
		if (blobName != null) {
			signedResource = "b";
			request.path(blobName);
		}

		DateTimeFormatter dtf = longFormat ? ISO8061_DATE_TIME : ISO8061_DATE;

		String startTimeStr = null;
		if (startTime == null) {
			startTime = ZonedDateTime.now(ZoneId.of("GMT"));
			//startTimeStr = ISO8061_DATE.format(startTime);// : ISO8061_DATE.format(startTime);
		} else {
			startTimeStr = dtf.format(startTime);// : ISO8061_DATE.format(startTime);
		}

		String expiryTimeStr = null;
		if (expiryDuration == null) {
			expiryDuration = Duration.ofHours(24);
		}
		ZonedDateTime expiryTime = startTime.plus(expiryDuration);
		expiryTimeStr = dtf.format(expiryTime);// : ISO8061_DATE.format(expiryTime);

		request.queryParam("sv", AZURE_STORAGE_VERSION);
		request.queryParam("sr", signedResource);
		if (startTimeStr != null) {
			request.queryParam("st", startTimeStr);
		}
		request.queryParam("se", expiryTimeStr);
		request.queryParam("sp", permissions);

		StringBuilder stringToSign = new StringBuilder();
		stringToSign.append(permissions).append("\n");
		stringToSign.append(startTimeStr != null ? startTimeStr : "").append("\n");
		stringToSign.append(expiryTimeStr).append("\n");
		stringToSign.append("/").append(id).append(request.build().getPath()).append("\n");
		stringToSign.append("\n");//no signature policy		
		stringToSign.append(AZURE_STORAGE_VERSION).append("\n");
		stringToSign.append("\n"); //No Cache Control
		stringToSign.append("\n"); //No Content Disposition
		stringToSign.append("\n"); //No Content Encoding
		stringToSign.append("\n"); //No Content Language
		//		/stringToSign.append("\n"); //No Content Type

		try {
			Mac sha256_HMAC = Mac.getInstance("HMACSHA256");
			SecretKeySpec secret_key = new SecretKeySpec(Base64.getDecoder().decode(masterKey), "HmacSHA256");
			sha256_HMAC.init(secret_key);
			String signature = Base64.getEncoder().encodeToString(sha256_HMAC.doFinal(stringToSign.toString().getBytes()));
			request.queryParam("sig", signature);
		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			throw new WebApplicationException(e);
		}

		return request.toString();
	}

	public <T> T operation(WebTarget target, String method, MultivaluedMap<String, Object> headers, int expectedStatus, Entity<?> requestContent, Class<T> responseType) throws WebApplicationException {
		Builder builder = target.request();
		if (headers == null) {
			headers = new MultivaluedHashMap<String, Object>();
		}

		setSharedKeyLiteHeader(target.getUri(), builder, method, requestContent != null ? requestContent.getMediaType().toString() : null, headers);
		builder.accept(MediaType.APPLICATION_XML_TYPE);
		Response response = builder.method(method, requestContent);

		//https://myaccount.blob.core.windows.net/mycontainer?restype=container

		if (response.getStatusInfo().getStatusCode() != expectedStatus) {
			throw new WebApplicationException(response);
		}

		if (response.hasEntity()) {
			if (responseType.isAssignableFrom(Response.class)) {
				return (T) response;
			}
			return response.readEntity(responseType);
		}

		return null;

	}

	//https://msdn.microsoft.com/en-us/library/azure/dd179428.aspx
	public void setSharedKeyLiteHeader(URI requestURI, Builder builder, String method, String contentType, MultivaluedMap<String, Object> headers) throws WebApplicationException {
		//x-ms-blob-public-access container blob
		ZonedDateTime currentTime = ZonedDateTime.now(ZoneId.of("GMT"));
		String date = DateTimeFormatter.RFC_1123_DATE_TIME.format(currentTime);

		headers.add("x-ms-date", date);
		headers.add("x-ms-version", AZURE_STORAGE_VERSION);

		builder.headers(headers);

		StringBuilder stringToSign = new StringBuilder();
		stringToSign.append(method).append("\n");
		stringToSign.append("\n"); //No Content-MD5
		stringToSign.append(contentType != null ? contentType : "").append("\n");
		canonicalize(stringToSign, headers); //CanonicalizedHeaders 
		stringToSign.append("\n"); //No Date
		stringToSign.append("/").append(id);
		if (requestURI.getPath() != null) {
			stringToSign.append(requestURI.getPath());
		}
		//canonicalize(stringToSign, parseQuery(requestURI.getQuery())); //apparently not needed with lite

		try {
			Mac sha256_HMAC = Mac.getInstance("HMACSHA256");
			SecretKeySpec secret_key = new SecretKeySpec(Base64.getDecoder().decode(masterKey), "HmacSHA256");
			sha256_HMAC.init(secret_key);
			String signature = Base64.getEncoder().encodeToString(sha256_HMAC.doFinal(stringToSign.toString().getBytes()));
			builder.header("Authorization", String.format("SharedKeyLite %s:%s", id, signature));
		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			throw new WebApplicationException(e);
		}

	}

	//TODO this could be implemented but it is challenging to get content-length and jax-rs overwrites some of the headers during posting
	public void setSharedKeyHeader(Builder builder, String method, MultivaluedMap<String, Object> headers) {

		ZonedDateTime currentTime = ZonedDateTime.now(ZoneId.of("GMT"));
		String date = DateTimeFormatter.RFC_1123_DATE_TIME.format(currentTime);
		StringBuilder authHeader = new StringBuilder();
		authHeader.append(method).append("\n");
		//authHeader.append(headers.getFirst(key)).append("\n");

		builder.headers(headers);
		builder.header("x-ms-date", date);
		builder.header("x-ms-version", AZURE_STORAGE_VERSION);

		//builder.header("Authorization", date);

	}

	public static void canonicalize(StringBuilder stringToSign, MultivaluedMap<String, Object> values) {
		//convert to lowercase
		MultivaluedHashMap<String, String> lc = new MultivaluedHashMap<String, String>();
		for (Map.Entry<String, List<Object>> entry : values.entrySet()) {
			if (entry.getValue() != null) {
				List<String> newValues = new ArrayList<String>(entry.getValue().size());
				for (Object o : entry.getValue()) {
					newValues.add(o.toString());
				}
				lc.put(entry.getKey().toLowerCase(), newValues);
			} else {
				lc.put(entry.getKey().toLowerCase(), null);
			}
		}
		List<String> sl = new ArrayList<>(lc.keySet());
		Collections.sort(sl);
		for (String key : sl) {
			stringToSign.append("\n");
			stringToSign.append(key).append(":");
			List<String> lcValues = lc.get(key);
			if (lcValues != null) {
				if (lcValues.size() == 1) {
					stringToSign.append(lcValues.get(0));
				} else {
					for (int i = 0; i < lcValues.size(); i++) {
						stringToSign.append(lcValues.get(i)).append("-").append(i + 1);
					}
				}
			}

		}

	}

	public static MultivaluedMap<String, Object> parseQuery(String query) throws WebApplicationException {
		MultivaluedHashMap<String, Object> result = new MultivaluedHashMap<String, Object>();
		if (query != null) {
			String[] pairs = query.split("&");
			for (String pair : pairs) {
				int index = pair.indexOf("=");
				try {
					String key = index > 0 ? URLDecoder.decode(pair.substring(0, index), "UTF-8") : pair;
					final String value = index > 0 && pair.length() > index + 1 ? URLDecoder.decode(pair.substring(index + 1), "UTF-8") : null;
					result.add(key, value);
				} catch (UnsupportedEncodingException ue) {
					throw new WebApplicationException(ue);
				}
			}
		}

		return result;
	}
}
