package gr.iti.mklab.focused.crawler.bolts.input;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.impl.TwitterRetriever;

/**
 * Class responsible for setting up the connection to Twitter API
 * for retrieving relevant Twitter content. Handles both the connection
 * to Twitter REST API and Twitter Subscriber. 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class TwitterStream extends Stream {
	
	public static Source SOURCE = Source.Twitter;
	
	private Logger  logger = Logger.getLogger(TwitterStream.class);

	@Override
	public synchronized void open(Configuration config) {

		logger.info("#Twitter : Open stream");
		
		if (config == null) {
			logger.error("#Twitter : Config file is null.");
			return;
		}
		
		String oAuthConsumerKey 		= 	config.getParameter(KEY);
		String oAuthConsumerSecret 		= 	config.getParameter(SECRET);
		String oAuthAccessToken 		= 	config.getParameter(ACCESS_TOKEN);
		String oAuthAccessTokenSecret 	= 	config.getParameter(ACCESS_TOKEN_SECRET);
		
		if (oAuthConsumerKey == null || oAuthConsumerSecret == null ||
				oAuthAccessToken == null || oAuthAccessTokenSecret == null) {
			logger.error("#Twitter : Stream requires authentication");
		}
		
		logger.info("Twitter Credentials: \n" + 
				"\t\t\toAuthConsumerKey:  " + oAuthConsumerKey  + "\n" +
				"\t\t\toAuthConsumerSecret:  " + oAuthConsumerSecret  + "\n" +
				"\t\t\toAuthAccessToken:  " + oAuthAccessToken + "\n" +
				"\t\t\toAuthAccessTokenSecret:  " + oAuthAccessTokenSecret);

		logger.info("Initialize Twitter Retriever for REST api");

		String maxRequests = config.getParameter(MAX_REQUESTS);
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
			
		Credentials credentials = new Credentials();
		credentials.setKey(oAuthConsumerKey);
		credentials.setSecret(oAuthConsumerSecret);
		credentials.setAccessToken(oAuthAccessToken);
		credentials.setAccessTokenSecret(oAuthAccessTokenSecret);
		
		retriever = new TwitterRetriever(credentials, Integer.parseInt(maxRequests), Long.parseLong(maxRunningTime));	
	}
	
	@Override
	public String getName() {
		return "Twitter";
	}
}

