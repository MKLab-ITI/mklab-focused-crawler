package gr.iti.mklab.focused.crawler.filters;

import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.Item;

public abstract class ItemFilter {
	
	@SuppressWarnings("unused")
	private Configuration configuration;

	private int discarded = 0;
	private int accepted = 0;

	public ItemFilter(Configuration configuration) {
		this.configuration = configuration;
	}
	
	public abstract boolean accept(Item item);
	
	public abstract String name();
	
	public String status() {
		return discarded + " items discarded, " + accepted + " items accepted.";
	}
	
	public void incrementAccepted() {
		synchronized (this) {
	    	accepted++;
	    }
	}
	
	public void incrementDiscarded() {
		synchronized (this) {
			discarded++;
	    }
	}
	
}
