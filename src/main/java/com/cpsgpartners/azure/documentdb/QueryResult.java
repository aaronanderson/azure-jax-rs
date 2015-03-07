package com.cpsgpartners.azure.documentdb;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryResult<R> {
	
	@JsonProperty("Documents")
	private List<R> documents;
	
	@JsonIgnore
	private String continuation;
	
	@JsonProperty("_rid")
	private String rId;

	public List<R> getDocuments() {
		return documents;
	}
	
	
	public void setDocuments(List<R> documents) {
		this.documents = documents;
	}

	public String getRId() {
		return rId;
	}

	public void setRid(String rId) {
		this.rId = rId;
	}

	public String getContinuation() {
		return continuation;
	}

	public void setContinuation(String continuation) {
		this.continuation = continuation;
	}

}