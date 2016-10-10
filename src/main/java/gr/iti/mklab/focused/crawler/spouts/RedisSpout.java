package gr.iti.mklab.focused.crawler.spouts;

import static org.apache.storm.utils.Utils.tuple;
import gr.iti.mklab.focused.crawler.utils.UrlStatusMonitor;
import gr.iti.mklab.focused.crawler.utils.UrlStatusMonitor.PROCESSING_STATUS;

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

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

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

	private long failed = 0, ack = 0, send = 0, received = 0;

	private UrlStatusMonitor urlStatus;
	
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
					received++;
					queue.offer(message);
				}

				@Override
				public void onPMessage(String pattern, String channel, String message) { 
					received++;
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
		
		urlStatus = new UrlStatusMonitor(pool);
		
		ListenerThread listener = new ListenerThread(queue, pool);
		listener.start();
		
	}

	public void close() {
		pool.destroy();
	}

	public void nextTuple() {
		
		String msg = queue.poll();
        if(msg != null) {
        	try {
        		DBObject obj = (DBObject) JSON.parse(msg);
        		
        		String url = (String) obj.get("url");
        		PROCESSING_STATUS status = urlStatus.getProcessingStatus(url);
        		logger.info(url + " => " + status);
        		if(status.equals(PROCESSING_STATUS.NEW)) {
        
        			send++;
        			_collector.emit(tuple(obj), url);
        			
        			urlStatus.setProcessingStatus(url, status);
        			
        			 if(send%100 == 0) {
        				 logger.info("send: " + send + ", received: " + received +  ", ack: " + ack + ", failed: " + failed + ", in queue: " + queue.size());
        			 }
        		}
        	}
        	catch(Exception e) {
        		e.printStackTrace();
        	}
        }
	}

	public void ack(Object msgId) {
		ack++;
		urlStatus.setProcessingStatus(msgId.toString(), PROCESSING_STATUS.PROCCESSED);
	}

	public void fail(Object msgId) {
		failed++;
		urlStatus.setProcessingStatus(msgId.toString(), PROCESSING_STATUS.FAILED);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(outputField));
	}

	public boolean isDistributed() {
		return false;
	}
	
}