package gr.iti.mklab.focused.crawler.bolts.input;

import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.impl.RssRetriever;

/**
 * Class responsible for setting up the connection for retrieving RSS feeds.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class RssStream extends Stream {
	
	public static Source SOURCE = Source.RSS;
	
	@Override
	public void open(Configuration config) {
		retriever = new RssRetriever();
	}

	@Override
	public String getName() {
		return "RSS";
	}

}
