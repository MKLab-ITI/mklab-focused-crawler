package gr.iti.mklab.focused.crawler.bolts.items;

import gr.iti.mklab.focused.crawler.config.CrawlerConfiguration;
import gr.iti.mklab.framework.common.domain.Item;

import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FilterBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5045947580716989342L;

	private Logger logger;
	//private Collection<ItemFilter> filters = new ArrayList<ItemFilter>();

	private CrawlerConfiguration config;
	private OutputCollector collector;
	
	public FilterBolt(CrawlerConfiguration config) {
		this.config = config;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		logger = Logger.getLogger(FilterBolt.class);
		
		this.collector = collector;
		createFilters(config);
	}

	@Override
	public void execute(Tuple input) {	
		Item item = (Item) input.getValueByField("Item");
		//for(ItemFilter filter : filters) {
		//	if(!filter.accept(item)) {
		//		return;
		//	}
		//}
		
		collector.emit(new Values(item));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Item"));
	}

	private void createFilters(CrawlerConfiguration config) {
		for (String filterId : config.getFilterIds()) {
			try {
				//Configuration fconfig = config.getFilterConfig(filterId);
				//String className = fconfig.getParameter(Configuration.CLASS_PATH);
				//Constructor<?> constructor = Class.forName(className).getConstructor(Configuration.class);
				//ItemFilter filterInstance = (ItemFilter) constructor.newInstance(fconfig);
			
				//filters.add(filterInstance);
			}
			catch(Exception e) {
				logger.error("Error during filter " + filterId + "initialization", e);
			}
		}
	}
	
}
