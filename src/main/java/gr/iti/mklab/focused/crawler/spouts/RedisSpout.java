package gr.iti.mklab.focused.crawler.spouts;

import static org.apache.storm.utils.Utils.tuple;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class RedisSpout extends BaseRichSpout {

	private Logger logger;
	
	static final long serialVersionUID = 737015318988609460L;
	
	private SpoutOutputCollector _collector;
	private final String host;
	private final int port;
	
	private final String pattern;
	
	private LinkedBlockingQueue<String> queue;
	private JedisPool pool;

	private String outputField;

	public RedisSpout(String host, int port, String pattern, String outputField) {
		this.host = host;
		this.port = port;
		this.pattern = pattern;
		this.outputField = outputField;
	}

	class ListenerThread extends Thread {
		
		private LinkedBlockingQueue<String> queue;
		private JedisPool pool;
			
		public ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool) {
			this.queue = queue;
			this.pool = pool;
		}

		public void run() {
			
			JedisPubSub listener = new JedisPubSub() {

				@Override
				public void onMessage(String channel, String message) {
					queue.offer(message);
				}

				@Override
				public void onPMessage(String pattern, String channel, String message) { 
					queue.offer(message);
				}

				@Override
				public void onPSubscribe(String channel, int subscribedChannels) { }

				@Override
				public void onPUnsubscribe(String channel, int subscribedChannels) { }

				@Override
				public void onSubscribe(String channel, int subscribedChannels) { }

				@Override
				public void onUnsubscribe(String channel, int subscribedChannels) { }
			
			};

			Jedis jedis = pool.getResource();
			try {
				logger.info("Subscribe on " + pattern);
				jedis.psubscribe(listener, pattern);
			} finally {
				pool.returnResource(jedis);
			}
		}
	};

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {

		logger = Logger.getLogger(RedisSpout.class);
		
		_collector = collector;
		queue = new LinkedBlockingQueue<String>(10000);
		
		JedisPoolConfig jedisConf = new JedisPoolConfig();
		pool = new JedisPool(jedisConf, host, port);
		
		ListenerThread listener = new ListenerThread(queue, pool);
		listener.start();
		
	}

	public void close() {
		pool.destroy();
	}

	public void nextTuple() {
		String msg = queue.poll();
        if(msg == null) {
            Utils.sleep(50);
        } else {
            _collector.emit(tuple(msg));            
        }
	}

	public void ack(Object msgId) {

	}

	public void fail(Object msgId) {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(outputField));
	}

	public boolean isDistributed() {
		return false;
	}
	
}