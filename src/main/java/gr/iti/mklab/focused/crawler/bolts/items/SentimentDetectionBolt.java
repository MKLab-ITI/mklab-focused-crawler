package gr.iti.mklab.focused.crawler.bolts.items;

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


public class SentimentDetectionBolt extends BaseRichBolt {

	private static final long serialVersionUID = 7935961067953158062L;

	private OutputCollector _collector;

	private Logger logger;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		this.logger = Logger.getLogger(SentimentDetectionBolt.class);
	}

	public void execute(Tuple input) {
		try {
			Item item = (Item)input.getValueByField("Item");
			
			//TODO: Detect Sentiment in item and update the corresponding field
		
			_collector.emit(new Values(item));
		}
		catch(Exception e){
			logger.error(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Item"));
	}

}
