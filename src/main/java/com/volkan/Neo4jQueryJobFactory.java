package com.volkan;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.volkan.interpartitiontraverse.JsonKeyConstants;

public class Neo4jQueryJobFactory {

	private static final Logger logger = LoggerFactory.getLogger(Neo4jQueryJobFactory.class);

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
					String port = fetchPortFromRedis(jedisPool, jsonMap);
					generateJobInDBFromJsonFileName(h2Client, jsonMap, port);
					Neo4jClientAsync neo4jClientAsync = new Neo4jClientAsync();
					neo4jClientAsync.delegateQueryAsync(port, jsonMap);
					logger.info("Job submitted to Neo-{}", port);
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
	
	public static Runnable buildJobWithGenarateJobForWOScotch(
			final H2Client h2Client, final Map<String, Object> jsonMap) {
		
		Runnable r = new Runnable() {
			
			@Override
			public void run() {
				try {
					String port = fetchPortViaModulo();
					generateJobInDBFromJsonFileName(h2Client, jsonMap, port);
					Neo4jClientAsync neo4jClientAsync = new Neo4jClientAsync();
					neo4jClientAsync.delegateQueryAsync(port, jsonMap);
					logger.info("Job submitted to Neo-{}", port);
					neo4jClientAsync.periodicFetcher((long) jsonMap.get(JsonKeyConstants.PARENT_JOB_ID));
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}

			private String fetchPortViaModulo(){
				int partitionCount = Integer.parseInt(
						Utility.getValueOfProperty("PARTITION_COUNT", "-1").toString());
				int startNodeID = 
						Integer.parseInt(jsonMap.get(JsonKeyConstants.START_NODE).toString());
				String port = startNodeID % (partitionCount) + 6474 + "";
				return port;
			}
			
		};
		
		return r;
	}
	
	private static Map<String, Object> generateJobInDBFromJsonFileName(
			H2Client h2Client, Map<String, Object> jsonMap, String port) throws Exception {

		jsonMap.put(JsonKeyConstants.START_PORT, port);
		long jobID = h2Client.generateJob(jsonMap);
		jsonMap.put(JsonKeyConstants.JOB_ID, jobID);
		jsonMap.put(JsonKeyConstants.PARENT_JOB_ID, jobID);
		return jsonMap;
	}
}
