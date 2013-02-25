package com.volkan;

import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

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

	public static Runnable buildJobWithGenarateJob(
			final H2Client h2Client, final JedisPool jedisPool, final Map<String, Object> jsonMap) {
		
		Runnable r = new Runnable() {
			
			@Override
			public void run() {
				try {
//					String port = jsonMap.get(JsonKeyConstants.START_PORT).toString();
					String port = fetchPortFromRedis(jedisPool, jsonMap);
					
					generateJobInDBFromJsonFileName(h2Client, jsonMap);
					Neo4jClientAsync neo4jClientAsync = new Neo4jClientAsync();
					neo4jClientAsync.delegateQueryAsync(port, jsonMap);
					neo4jClientAsync.periodicFetcher((long) jsonMap.get(JsonKeyConstants.PARENT_JOB_ID));
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}

			private String fetchPortFromRedis(final JedisPool jedisPool,
					final Map<String, Object> jsonMap) {
				Jedis jedis = jedisPool.getResource();
				String port = jedis.get("gid:"+jsonMap.get(JsonKeyConstants.START_NODE));
				jedisPool.returnResource(jedis);
				return port;
			}
		};
		
		return r;
	}
	
	private static Map<String, Object> generateJobInDBFromJsonFileName(
			H2Client h2Client, Map<String, Object> jsonMap) throws Exception {
		long jobID = h2Client.generateJob(jsonMap);
		jsonMap.put(JsonKeyConstants.JOB_ID, jobID);
		jsonMap.put(JsonKeyConstants.PARENT_JOB_ID, jobID);
		return jsonMap;
	}
}
