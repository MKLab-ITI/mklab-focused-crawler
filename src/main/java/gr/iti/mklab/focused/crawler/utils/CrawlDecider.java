package gr.iti.mklab.focused.crawler.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class CrawlDecider {

	private String host;

	private JedisPool pool;
	private Jedis jedis;
	
	private static Integer expirationTime = 24 * 3600;
	
	public CrawlDecider(String redisHost) {
		host = redisHost;
		pool = new JedisPool(new JedisPoolConfig(), host);
		jedis = pool.getResource();

	}

	public void setStatus(String id, String status) {
		jedis.hset(id, "status", status);
		jedis.expire(id, expirationTime);
		
	}
	
	public String getStatus(String id) {
		String value = jedis.hget(id, "status");
		return value;
	}
	
	public void deleteStatus(String id) {
		jedis.del(id);
	}

	
}
