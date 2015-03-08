package com.cpsgpartners.azure.documentdb;

import java.util.List;

import com.cpsgpartners.azure.documentdb.DocumentDB.QueryResult;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JQueryResult<R> implements QueryResult<R> {

	@JsonProperty("Documents")
	private List<R> documents;

	@JsonIgnore
	private String continuation;

	@JsonProperty("_rid")
	private String rId;

	@JsonProperty("_count")
	private int count;

	public List<R> getDocuments() {
		return documents;
	}

	public void setDocuments(List<R> documents) {
		this.documents = documents;
	}

	public String getRId() {
		return rId;
	}

	public void setRId(String rId) {
		this.rId = rId;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getContinuation() {
		return continuation;
	}

	public void setContinuation(String continuation) {
		this.continuation = continuation;
	}

}
