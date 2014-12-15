package gr.iti.mklab.focused.crawler.input;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.impl.FlickrRetriever;

/**
 * Class responsible for setting up the connection to Flickr API
 * for retrieving relevant Flickr content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class FlickrStream extends Stream {

	private Logger logger = Logger.getLogger(FlickrStream.class);
	
	public static final Source SOURCE = Source.Flickr;
	
	private String key;
	private String secret;

	
	@Override
	public void open(Configuration config) {
		logger.info("#Flickr : Open stream");
		
		if (config == null) {
			logger.error("#Flickr : Config file is null.");
			return;
		}
		
		key = config.getParameter(KEY);
		secret = config.getParameter(SECRET);
		
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRequests = config.getParameter(MAX_REQUESTS);
		
		if (key == null || secret==null) {
			logger.error("#Flickr : Stream requires authentication.");
		}
		
		Credentials credentials = new Credentials();
		credentials.setKey(key);
		credentials.setSecret(secret);
		
		retriever = new FlickrRetriever(credentials, Integer.parseInt(maxResults), Long.parseLong(maxRequests));
		
	}
	
	@Override
	public String getName() {
		return "Flickr";
	}
	
}
