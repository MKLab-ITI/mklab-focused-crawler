package gr.iti.mklab.focused.crawler.bolts.input;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.RateLimitsMonitor;
import gr.iti.mklab.framework.retrievers.impl.TumblrRetriever;

/**
 * Class responsible for setting up the connection to Tumblr API
 * for retrieving relevant Tumblr content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class TumblrStream extends Stream {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7484340079911488711L;

	public static final Source SOURCE = Source.Tumblr;
	
	private String consumerKey;
	private String consumerSecret;
	
	private Logger logger = Logger.getLogger(TumblrStream.class);

	public TumblrStream(Configuration config) {
		super(config);
	}
	
	@Override
	public void open(Configuration config) {
		logger.info("#Tumblr : Open stream");
		
		if (config == null) {
			logger.error("#Tumblr : Config file is null.");
			return;
		}
		
		consumerKey = config.getParameter(KEY);
		consumerSecret = config.getParameter(SECRET);
		
		int maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		long windowLength = Long.parseLong(config.getParameter(WINDOW_LENGTH));
		
		if (consumerKey == null || consumerSecret==null) {
			logger.error("#Tumblr : Stream requires authentication.");
		}
		
		Credentials credentials = new Credentials();
		credentials.setKey(consumerKey);
		credentials.setSecret(consumerSecret);
		
		RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(maxRequests, windowLength);
		retriever = new TumblrRetriever(credentials, rateLimitsMonitor);
		
	}

	@Override
	public String getName() {
		return "Tumblr";
	}

}
