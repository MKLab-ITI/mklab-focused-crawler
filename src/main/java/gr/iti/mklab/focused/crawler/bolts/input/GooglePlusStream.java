package gr.iti.mklab.focused.crawler.bolts.input;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.RateLimitsMonitor;
import gr.iti.mklab.framework.retrievers.impl.GooglePlusRetriever;

/**
 * Class responsible for setting up the connection to Google API
 * for retrieving relevant Google+ content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class GooglePlusStream extends Stream {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8806944911389188798L;
	public static final Source SOURCE = Source.GooglePlus;
	
	private Logger logger = Logger.getLogger(GooglePlusStream.class);

	public GooglePlusStream(Configuration config) {
		super(config);
	}
	
	@Override
	public void open(Configuration config) {
		logger.info("#GooglePlus : Open stream");
		
		if (config == null) {
			logger.error("#GooglePlus : Config file is null.");
			return;
		}
		
		String key = config.getParameter(KEY);
		
		int maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		long windowLength = Long.parseLong(config.getParameter(WINDOW_LENGTH));
		
		if (key == null) {
			logger.error("#GooglePlus : Stream requires authentication.");
		}
		
		Credentials credentials = new Credentials();
		credentials.setKey(key);
		
		RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(maxRequests, windowLength);
		retriever = new GooglePlusRetriever(credentials, rateLimitsMonitor);
		
	}
	
	@Override
	public String getName() {
		return "GooglePlus";
	}
}
