package com.cpsgpartners.azure.documentdb;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import javax.ws.rs.core.GenericType;

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

	public static final <T> GenericType<JQueryResult<T>> genericType(Class<T> type) {
		return new GenericType<JQueryResult<T>>(new JQueryResultParameterizedType(type));
	}

	public static class JQueryResultParameterizedType implements ParameterizedType {
		Type[] type;

		public JQueryResultParameterizedType(Class<?> type) {
			this.type = new Type[] { type };
		}

		@Override
		public Type[] getActualTypeArguments() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Type getRawType() {
			return JQueryResult.class;
		}

		@Override
		public Type getOwnerType() {
			return JQueryResult.class;
		}

	}

}
