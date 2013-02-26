package com.volkan;

import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.volkan.interpartitiontraverse.JsonHelper;
import com.volkan.interpartitiontraverse.JsonKeyConstants;

public class JedisDeneme {

	public static void main(String[] args) throws Exception {
		final JedisPool jedisPool = new JedisPool("localhost", 6379);
		Jedis jedis = jedisPool.getResource();
		
		String jsonFileName = "/home/volkan/Documents/workspace-sts-2.7.2.RELEASE/" +
				"neowithmaven20121016/src/main/resources/jsons/erdos6474_6_143.json";
		Map<String, Object> jsonMap = JsonHelper.readJsonFileIntoMap(jsonFileName);
		String port = jedis.get("gid:"+jsonMap.get(JsonKeyConstants.START_NODE));
		System.out.println(port);
		jedisPool.returnResource(jedis);
	}
}
