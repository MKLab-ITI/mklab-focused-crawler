package gr.iti.mklab.focused.crawler.bolts.input;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.RateLimitsMonitor;
import gr.iti.mklab.framework.retrievers.impl.YoutubeRetriever;

/**
 * Class responsible for setting up the connection to Google API
 * for retrieving relevant YouTube content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class YoutubeStream extends Stream {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6164270228594034798L;

	public static Source SOURCE = Source.Youtube;
	
	private Logger logger = Logger.getLogger(YoutubeStream.class);
	
	private String clientId;
	private String developerKey;
	
	public YoutubeStream(Configuration config) {
		super(config);
	}
	
	@Override
	public void open(Configuration config) {
		logger.info("#YouTube : Open stream");
		
		if (config == null) {
			logger.error("#YouTube : Config file is null.");
			return;
		}
		
		this.clientId = config.getParameter(CLIENT_ID);
		this.developerKey = config.getParameter(KEY);
		
		int maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		long windowLength = Long.parseLong(config.getParameter(WINDOW_LENGTH));
		
		if (clientId == null || developerKey == null) {
			logger.error("#YouTube : Stream requires authentication.");
		}

		Credentials credentials = new Credentials();
		credentials.setKey(developerKey);
		credentials.setClientId(clientId);
		
		RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(maxRequests, windowLength);
		retriever = new YoutubeRetriever(credentials, rateLimitsMonitor);
	}
	
	@Override
	public String getName() {
		return "YouTube";
	}
	
}
