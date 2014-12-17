package gr.iti.mklab.focused.crawler.bolts.input;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.RateLimitsMonitor;
import gr.iti.mklab.framework.retrievers.impl.FacebookRetriever;

/**
 * Class responsible for setting up the connection to Facebook API
 * for retrieving relevant Facebook content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class FacebookStream extends Stream {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 203079382776859219L;

	public static Source SOURCE = Source.Facebook;
	
	private Logger  logger = Logger.getLogger(FacebookStream.class);	
	
	public FacebookStream(Configuration config) {
		super(config);
	}
	
	@Override
	public synchronized void open(Configuration config) {
		logger.info("#Facebook : Open stream");
		
		if (config == null) {
			logger.error("#Facebook : Config file is null.");
			return;
		}
		
		
		String accessToken = config.getParameter(ACCESS_TOKEN);
		String app_id = config.getParameter(APP_ID);
		String app_secret = config.getParameter(APP_SECRET);
		
		int maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		long windowLength = Long.parseLong(config.getParameter(WINDOW_LENGTH));
		
		if (accessToken == null && app_id == null && app_secret == null) {
			logger.error("#Facebook : Stream requires authentication.");
		}
		
		
		if(accessToken == null)
			accessToken = app_id+"|"+app_secret;
		
		Credentials credentials = new Credentials();
		credentials.setAccessToken(accessToken);
		
		RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(maxRequests, windowLength);
		retriever = new FacebookRetriever(credentials, rateLimitsMonitor);	

	}

	@Override
	public String getName() {
		return "Facebook";
	}
	
}