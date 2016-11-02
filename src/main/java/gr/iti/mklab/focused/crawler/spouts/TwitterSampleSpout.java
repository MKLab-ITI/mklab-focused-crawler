package gr.iti.mklab.focused.crawler.spouts;
import gr.iti.mklab.framework.abstractions.socialmedia.items.TwitterItem;
import gr.iti.mklab.framework.common.domain.Item;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;


public class TwitterSampleSpout extends BaseRichSpout {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5781934512217875334L;

	private long failed = 0, ack = 0, send = 0, received = 0, start = 0;
	
	private SpoutOutputCollector _collector;
	private LinkedBlockingQueue<Item> _queue = null;
	private TwitterStream _twitterStream;
		
	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessTokenSecret;
	private String[] keyWords;

	private Logger _logger;
		
	public TwitterSampleSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
	}
	
	public TwitterSampleSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret, String[] keyWords) {
         this.consumerKey = consumerKey;
         this.consumerSecret = consumerSecret;
         this.accessToken = accessToken;
         this.accessTokenSecret = accessTokenSecret;
         this.keyWords = keyWords;
	}
		
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		_logger = Logger.getLogger(TwitterSampleSpout.class);
		
		_queue = new LinkedBlockingQueue<Item>(2000);
		_collector = collector;
		StatusListener listener = new StatusListener() {
			@Override
            public void onStatus(Status status) {
				if(status.getLang().equals("en")) {
                                    received++;
					Item item = new TwitterItem(status);
					_queue.offer(item);
				}
            }
					
            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {}
					
            @Override
            public void onTrackLimitationNotice(int i) {}
					
            @Override
            public void onScrubGeo(long l, long l1) {}
					
            @Override
            public void onException(Exception ex) {}
					
            @Override
            public void onStallWarning(StallWarning arg0) {}
         };
				
         ConfigurationBuilder cb = new ConfigurationBuilder();
				
         cb.setDebugEnabled(true)
            .setOAuthConsumerKey(consumerKey)
            .setOAuthConsumerSecret(consumerSecret)
            .setOAuthAccessToken(accessToken)
            .setOAuthAccessTokenSecret(accessTokenSecret);
					
         _twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
         _twitterStream.addListener(listener);
         			
         if (keyWords == null || keyWords.length == 0) {
            _twitterStream.sample();
         }
         else {
            FilterQuery query = new FilterQuery().track(keyWords);
            _twitterStream.filter(query);
         }
         
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        Runnable statistics = () -> {
            
            _logger.info("Crawl rate: " + ((float)received/(++start)) + " items/min.");
        };
        executor.scheduleAtFixedRate(statistics, 1, 1, TimeUnit.MINUTES);
	}
			
	@Override
	public void nextTuple() {
		Item item = _queue.poll();
				
		if (item == null) {
			Utils.sleep(50);
		} 
		else {
			send++;
			_collector.emit(new Values(item), item.getId());
		}
	}
			
	@Override
	public void close() {
		_twitterStream.shutdown();
	}
			
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}
			
	@Override
	public void ack(Object id) {
		ack++;
	}
			
	@Override
	public void fail(Object id) {
		failed++;
		_logger.info(id + " failed to be processed.");
	}
			
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Item"));
	}
   
}