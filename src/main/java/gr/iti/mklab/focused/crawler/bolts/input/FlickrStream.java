package gr.iti.mklab.focused.crawler.bolts.input;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.RateLimitsMonitor;
import gr.iti.mklab.framework.retrievers.impl.FlickrRetriever;

/**
 * Class responsible for setting up the connection to Flickr API
 * for retrieving relevant Flickr content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class FlickrStream extends Stream {

	/**
	 * 
	 */
	private static final long serialVersionUID = -703417216573946130L;

	private Logger logger = Logger.getLogger(FlickrStream.class);
	
	public static final Source SOURCE = Source.Flickr;

	public FlickrStream(Configuration config) {
		super(config);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void open(Configuration config) {
		logger.info("#Flickr : Open stream");
		
		if (config == null) {
			logger.error("#Flickr : Config file is null.");
			return;
		}
		
		String key = config.getParameter(KEY);
		String secret = config.getParameter(SECRET);
		
		int maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		long windowLength = Long.parseLong(config.getParameter(WINDOW_LENGTH));
		
		if (key == null || secret==null) {
			logger.error("#Flickr : Stream requires authentication.");
		}
		
		Credentials credentials = new Credentials();
		credentials.setKey(key);
		credentials.setSecret(secret);
		
		RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(maxRequests, windowLength);
		retriever = new FlickrRetriever(credentials, rateLimitsMonitor);
		
	}
	
	@Override
	public String getName() {
		return "Flickr";
	}
	
}
