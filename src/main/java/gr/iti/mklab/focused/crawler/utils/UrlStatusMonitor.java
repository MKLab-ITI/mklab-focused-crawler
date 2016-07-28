package gr.iti.mklab.focused.crawler.utils;

import java.io.Serializable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class UrlStatusMonitor implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3351757151725451267L;

	public static enum PROCESSING_STATUS {
		NEW,
		INJECTED,
		PROCCESSED,
		FAILED
	};

	public static enum CRAWL_STATUS {
		NEW,
		QUEUED,
		CRAWLED,
		FAILED
	};
	
	private Jedis primaryJedis, secondaryJedis;
	
	private static Integer expirationTime = 5 * 24 * 3600;
	
	public UrlStatusMonitor(String redisHost) {
		JedisPool pool = new JedisPool(new JedisPoolConfig(), redisHost);
		primaryJedis = pool.getResource();
		
		secondaryJedis = pool.getResource();
		secondaryJedis.select(1);
	}

	public UrlStatusMonitor(JedisPool pool) {
		primaryJedis = pool.getResource();
		
		secondaryJedis = pool.getResource();
		secondaryJedis.select(1);
	}

	
	public void setProcessingStatus(String id, PROCESSING_STATUS status) {
		primaryJedis.hset(id, "status", status.name());
		primaryJedis.expire(id, expirationTime);
	}
	
	public PROCESSING_STATUS getProcessingStatus(String id) {
		String value = primaryJedis.hget(id, "status");
		if(value == null) {
			return PROCESSING_STATUS.NEW;
		}
		
		PROCESSING_STATUS status = PROCESSING_STATUS.valueOf(value);
		return status;
	}
	
	public void setCrawlStatus(String id, CRAWL_STATUS status) {
		secondaryJedis.hset(id, "status", status.name());
		secondaryJedis.expire(id, expirationTime);
	}
	
	public CRAWL_STATUS getCrawlStatus(String id) {
		String value = secondaryJedis.hget(id, "status");
		if(value == null) {
			return CRAWL_STATUS.NEW;
		}
		
		CRAWL_STATUS status = CRAWL_STATUS.valueOf(value);
		return status;
	}
	
	public void deleteProcessingStatus(String id) {
		primaryJedis.del(id);
	}

	
}
