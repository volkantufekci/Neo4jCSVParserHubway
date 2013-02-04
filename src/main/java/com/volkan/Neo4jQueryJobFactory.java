package com.volkan;

import java.util.Map;

import com.volkan.interpartitiontraverse.JsonKeyConstants;

public class Neo4jQueryJobFactory {

	public static Runnable buildJob(final String port, final Map<String, Object> jsonMap) {
		Runnable r = new Runnable() {
			
			@Override
			public void run() {
				try {
					Neo4jClientAsync neo4jClientAsync = new Neo4jClientAsync();
					neo4jClientAsync.delegateQueryAsync(port, jsonMap);
					neo4jClientAsync.periodicFetcher((long) jsonMap.get(JsonKeyConstants.PARENT_JOB_ID));
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
		};
		
		return r;
	}
}
