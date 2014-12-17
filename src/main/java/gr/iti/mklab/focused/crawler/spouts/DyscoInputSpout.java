package gr.iti.mklab.focused.crawler.spouts;

import static backtype.storm.utils.Utils.tuple;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.QueryResults;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import gr.iti.mklab.framework.client.mongo.DAOFactory;
import gr.iti.mklab.framework.common.domain.Location;
import gr.iti.mklab.framework.common.domain.Account;
import gr.iti.mklab.framework.common.domain.dysco.Dysco;
import gr.iti.mklab.framework.common.domain.feeds.AccountFeed;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.common.util.DateUtil;

/**
 * @brief The class that is responsible for the creation of input feeds from DySco content
 * 
 * @author Manos Schinas
 * @email  manosetro@iti.gr
 */
public class DyscoInputSpout extends BaseRichSpout {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3871696088815378055L;

	private DateUtil dateUtil = new DateUtil();

	private SpoutOutputCollector _collector;
	private LinkedBlockingQueue<Dysco> _queue;

	private String hostname;
	private String dbName;

	private Logger _logger;

	private BasicDAO<Dysco, String> dyscoDAO;
		
	public DyscoInputSpout(String hostname, String dbName) {
		this.hostname = hostname;
		this.dbName = dbName;
	}
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		_logger = Logger.getLogger(DyscoInputSpout.class);
		
		_collector = collector;
		_queue = new LinkedBlockingQueue<Dysco>(1000);
		
		DAOFactory daoFactory = new DAOFactory();
		try {
			dyscoDAO = daoFactory.getDAO(hostname, dbName, Dysco.class);
			
			Reader reader = new Reader(dyscoDAO, _queue);
			Thread thread = new Thread(reader);			
			thread.start();
			
		} catch (Exception e) {
			e.printStackTrace();
			_logger.error(e);
		}
	
	}

	@Override
	public void nextTuple() {
		Dysco dysco = _queue.poll();
		if(dysco == null) {
            Utils.sleep(100);
        } else {    	
        	List<Feed> feeds = createFeeds(dysco);
        	
        	_logger.info(feeds.size() + " feeds created from Dysco: " + dysco.getId());
        	_collector.emit(tuple(feeds));
        }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("feeds"));		
	}

	class Reader implements Runnable {

		private Date since = new Date(0L);
		private BasicDAO<Dysco, String> dyscoDAO;
		private BlockingQueue<Dysco> queue;

		public Reader(BasicDAO<Dysco, String> dyscoDAO, BlockingQueue<Dysco> queue) {
			this.dyscoDAO = dyscoDAO;
			this.queue = queue;
		}

		@Override
		public void run() {
			Query<Dysco> query = dyscoDAO.createQuery();
			query.filter("updateDate", since);
			
			_logger.info(dyscoDAO.count(query) + " dyscos to process");
			
			QueryResults<Dysco> results = dyscoDAO.find(query);
			Iterator<Dysco> it = results.iterator();
			while(it.hasNext()) {
				Dysco dysco = it.next();
				if(dysco != null) {
					
					Date updateDate = dysco.getUpdateDate();
					if(updateDate.after(since)) {
						since.setTime(updateDate.getTime());
					}
					
					queue.add(dysco);
				}
			}
		}
		
	}
	
	private List<Feed> createFeeds(Dysco dysco) {
		
		Date sinceDate = dysco.getSinceDate();
		if(sinceDate == null) {
			sinceDate = dateUtil.addDays(dysco.getCreationDate(), -5);
		}
		
		List<Feed> feeds = new ArrayList<Feed>();
		
		List<String> words = dysco.getWords();
		if(words != null && !words.isEmpty()) {
			String feedID = UUID.randomUUID().toString();
			KeywordsFeed keywordsFeed = new KeywordsFeed(words, sinceDate, feedID);
			feeds.add(keywordsFeed);
		}
		
		List<Account> accounts = dysco.getAccounts();
		if(accounts != null && !accounts.isEmpty()) {
			for(Account account : accounts) {
				String feedID = UUID.randomUUID().toString();
				AccountFeed sourceFeed = new AccountFeed(account, sinceDate, feedID);
				feeds.add(sourceFeed);
			}
		}
		
		List<Location> nearLocations = dysco.getNearLocations();
		if(nearLocations != null && !nearLocations.isEmpty()) {
			for(Location location : nearLocations) {
				String feedID = UUID.randomUUID().toString();
				LocationFeed locationFeed = new LocationFeed(location, sinceDate, feedID);
				feeds.add(locationFeed);
			}
		}	
		
		// ADD GROUP FEEDS
		
		return feeds;
	}
	
}