package gr.iti.mklab.focused.crawler.bolts.input;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.impl.GooglePlusRetriever;

/**
 * Class responsible for setting up the connection to Google API
 * for retrieving relevant Google+ content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class GooglePlusStream extends Stream {
	public static final Source SOURCE = Source.GooglePlus;
	
	private Logger logger = Logger.getLogger(GooglePlusStream.class);
	
	private String key;

	@Override
	public void open(Configuration config) {
		logger.info("#GooglePlus : Open stream");
		
		if (config == null) {
			logger.error("#GooglePlus : Config file is null.");
			return;
		}
		
		key = config.getParameter(KEY);
		
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
		
		if (key == null) {
			logger.error("#GooglePlus : Stream requires authentication.");
		}
		
		Credentials credentials = new Credentials();
		credentials.setKey(key);
		
		retriever = new GooglePlusRetriever(credentials, 
				Integer.parseInt(maxResults), Long.parseLong(maxRunningTime));
		
	}
	
	@Override
	public String getName() {
		return "GooglePlus";
	}
}
