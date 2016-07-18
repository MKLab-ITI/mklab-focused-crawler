package gr.iti.mklab.focused.crawler.bolts.media;

import gr.iti.mklab.framework.common.domain.MediaItem;

import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class RedisBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Jedis publisherJedis;
	private String host, channel;

	private Logger logger;
	
	public RedisBolt(String host, String channel) {
		this.host = host;
		this.channel = channel;
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {

		JedisPoolConfig poolConfig = new JedisPoolConfig();
        JedisPool jedisPool = new JedisPool(poolConfig, host, 6379, 0);
		
        publisherJedis = jedisPool.getResource();
        logger = Logger.getLogger(RedisBolt.class);
	}

	public void execute(Tuple input) {
		try {
			MediaItem mi = (MediaItem) input.getValueByField("MediaItem");
			if(mi != null) {
				publisherJedis.publish(channel, mi.getId());
			}
		}
		catch(Exception e) {
			logger.error(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
