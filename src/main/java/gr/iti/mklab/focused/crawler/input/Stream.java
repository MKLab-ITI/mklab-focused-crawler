package gr.iti.mklab.focused.crawler.input;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import gr.iti.mklab.focused.crawler.input.monitor.FeedsMonitor;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.framework.retrievers.Retriever;

/**
 * Class responsible for handling the stream of information regarding 
 * a social network or a news feed source.
 * It is responsible for the configuration of the connection to the selected API
 * and the retrieval/storing of relevant content.
 * @author manosetro
 * @email  manosetro@iti.gr
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public abstract class Stream {//implements Runnable {

	protected static final String KEY = "Key";
	protected static final String SECRET = "Secret";
	protected static final String ACCESS_TOKEN = "AccessToken";
	protected static final String ACCESS_TOKEN_SECRET = "AccessTokenSecret";
	protected static final String CLIENT_ID = "ClientId";
	protected static final String APP_ID = "AppId";
	protected static final String APP_SECRET = "AppSecret";
	
	protected static final String MAX_RESULTS = "maxResults";
	protected static final String MAX_REQUESTS = "maxRequests";
	protected static final String MAX_RUNNING_TIME = "maxRunningTime";
	
	protected FeedsMonitor monitor;
	
	protected BlockingQueue<Feed> feedsQueue;
	protected Retriever retriever = null;
	
	private Logger  logger = Logger.getLogger(Stream.class);
	
	/**
	 * Opens a stream for updates delivery
	 * @param config
	 *      Stream configuration parameters
	 * @throws StreamException
	 *      In any case of error during stream open
	 */
	public abstract void open(Configuration config);
	
	/**
	 * Closes a stream 
	 * @throws StreamException
	 *      In any case of error during stream close
	 */
	public void close() {
	
		if(monitor != null) {
			logger.info("Stop monitor");
			monitor.stopMonitor();
		}
		
		if(retriever != null) {
			logger.info("Stop retriever");
			retriever.stop();
		}
		
		logger.info("Close Stream  : " + this.getClass().getName());
	}
	
	/**
	 * Sets the feeds monitor for the stream
	 * @return
	 */
	public boolean setMonitor() {
		if(retriever == null)
			return false;
		
		monitor = new FeedsMonitor(retriever);
		return true;
	}
	
	/**
	 * Sets the users list that will be used to retrieve from the stream (utilized for Twitter Stream)
	 * @param usersToLists
	 
	public void setUserLists(Map<String, Set<String>> usersToLists) {
		this.usersToLists = usersToLists;
		
		if(usersToLists != null) {
			Set<String> allLists = new HashSet<String>();
			for(Set<String> lists : usersToLists.values()) {
				allLists.addAll(lists);
			}
			logger.info("=============================================");
			logger.info(usersToLists.size() + " user in " + allLists.size() + " Lists!!!");
		}
	}
	*/
	
	/**
	 * Searches with the wrapper of the stream for a particular
	 * set of feeds (feeds can be keywordsFeeds, userFeeds, locationFeeds, listFeeds or URLFeeds)
	 * @param feeds
	 * @throws StreamException
	 */
	public synchronized List<Item> poll(List<Feed> feeds) {
		List<Item> retrievedItems = new ArrayList<Item>();
		if(retriever != null) {
		
			if(feeds == null) {
				logger.error("Feeds is null in poll method.");
				return retrievedItems;
			}
			
			//logger.info(getName() + ": poll for " + feeds.size() + " feeds");
			for(Feed feed : feeds) {
				try {
					List<Item> items = retriever.retrieve(feed);
		
					retrievedItems.addAll(items);
				}
				catch(Exception e) {
					logger.error("Exception for feed " + feed.getId() + " of type " + feed.getFeedtype() + " from "
							+ getName());
					logger.error("Feed: " + feed);
					logger.error(e.getMessage());
				}
			}
		}
		
		return retrievedItems;
	}
	
	/**
	 * Searches with the wrapper of the stream for a particular
	 * feed (feed can be keywordsFeeds, userFeeds, locationFeeds, listFeeds or URLFeeds)
	 * @param feeds
	 * @throws StreamException
	 */
	public synchronized List<Item> poll(Feed feed) {
		List<Item> retrievedItems = new ArrayList<Item>();
		if(retriever != null) {
		
			if(feed == null) {
				logger.error("Feeds is null in poll method.");
				return retrievedItems;
			}
			
			try {
				List<Item> items = retriever.retrieve(feed);

				retrievedItems.addAll(items);
			}
			catch(Exception e) {
				logger.error("Exception for feed " + feed.getId() + " of type " + feed.getFeedtype());
				logger.error(e.getMessage());
			}
		}
		
		return retrievedItems;
	}
	
	/**
	 * Adds a feed to the stream for future searching
	 * @param feed
	 * @return
	 */
	public boolean addFeed(Feed feed) {
		if(feedsQueue == null) {
			return false;
		}
		
		return feedsQueue.offer(feed);
	}
	/**
	 * Adds a set of feeds to the stream for future searching
	 * @param feeds
	 * @return
	 */
	public boolean addFeeds(List<Feed> feeds) {
		for(Feed feed : feeds) {
			if(!addFeed(feed)) {
				return false;
			}
		}
		return true;
	}
	
	public abstract String getName();
	
}
