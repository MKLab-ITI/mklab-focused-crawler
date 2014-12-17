package gr.iti.mklab.focused.crawler.bolts.input;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.impl.InstagramRetriever;

/**
 * Class responsible for setting up the connection to Instagram API
 * for retrieving relevant Instagram content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */

public class InstagramStream extends Stream {
	
	private Logger logger = Logger.getLogger(InstagramStream.class);
	
	public static final Source SOURCE = Source.Instagram;


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
		
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRequests = config.getParameter(MAX_REQUESTS);
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
		
		if (key == null || secret == null || token == null) {
			logger.error("#Instagram : Stream requires authentication.");
		}
		
		retriever = new InstagramRetriever(key, secret, token);
	
	}


	@Override
	public String getName() {
		return "Instagram";
	}
	
}

