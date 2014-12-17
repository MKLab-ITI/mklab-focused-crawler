package gr.iti.mklab.focused.crawler.bolts.input;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.RateLimitsMonitor;
import gr.iti.mklab.framework.retrievers.impl.InstagramRetriever;

/**
 * Class responsible for setting up the connection to Instagram API
 * for retrieving relevant Instagram content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */

public class InstagramStream extends Stream {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4162846494100416523L;
	public static final Source SOURCE = Source.Instagram;

	private Logger logger = Logger.getLogger(InstagramStream.class);
	
	public InstagramStream(Configuration config) {
		super(config);
	}
	
	@Override
	public void open(Configuration config) {
		logger.info("#Instagram : Open stream");
		
		if (config == null) {
			logger.error("#Instagram : Config file is null.");
			return;
		}
		
		String key = config.getParameter(KEY);
		String secret = config.getParameter(SECRET);
		String token = config.getParameter(ACCESS_TOKEN);
		
		int maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		long windowLength = Long.parseLong(config.getParameter(WINDOW_LENGTH));
		
		if (key == null || secret == null || token == null) {
			logger.error("#Instagram : Stream requires authentication.");
		}
		
		Credentials credentials = new Credentials();
		credentials.setKey(key);
		credentials.setSecret(secret);
		credentials.setAccessToken(token);
		
		RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(maxRequests, windowLength);
		retriever = new InstagramRetriever(credentials, rateLimitsMonitor);
	
	}


	@Override
	public String getName() {
		return "Instagram";
	}
	
}

