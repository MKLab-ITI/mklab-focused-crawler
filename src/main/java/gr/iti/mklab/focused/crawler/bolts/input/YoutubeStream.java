package gr.iti.mklab.focused.crawler.bolts.input;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.impl.YoutubeRetriever;

/**
 * Class responsible for setting up the connection to Google API
 * for retrieving relevant YouTube content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class YoutubeStream extends Stream {

	public static Source SOURCE = Source.Youtube;
	
	private Logger logger = Logger.getLogger(YoutubeStream.class);
	
	private String clientId;
	private String developerKey;
	
	@Override
	public void open(Configuration config) {
		logger.info("#YouTube : Open stream");
		
		if (config == null) {
			logger.error("#YouTube : Config file is null.");
			return;
		}
		
		this.clientId = config.getParameter(CLIENT_ID);
		this.developerKey = config.getParameter(KEY);
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
		
		if (clientId == null || developerKey == null) {
			logger.error("#YouTube : Stream requires authentication.");
		}

		Credentials credentials = new Credentials();
		credentials.setKey(developerKey);
		credentials.setClientId(clientId);
		
		retriever = new YoutubeRetriever(credentials, Integer.parseInt(maxResults),
				Long.parseLong(maxRunningTime));

	}
	
	@Override
	public String getName() {
		return "YouTube";
	}
	
}
