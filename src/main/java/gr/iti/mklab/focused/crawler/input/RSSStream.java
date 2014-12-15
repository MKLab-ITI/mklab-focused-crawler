package gr.iti.mklab.focused.crawler.input;

import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.impl.RSSRetriever;

/**
 * Class responsible for setting up the connection for retrieving RSS feeds.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class RSSStream extends Stream {
	
	public static Source SOURCE = Source.RSS;
	
	@Override
	public void open(Configuration config) {
		retriever = new RSSRetriever();
	}

	@Override
	public String getName() {
		return "RSS";
	}

}
