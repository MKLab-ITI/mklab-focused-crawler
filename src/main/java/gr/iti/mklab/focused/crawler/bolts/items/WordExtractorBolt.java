package gr.iti.mklab.focused.crawler.bolts.items;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.NamedEntity;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordExtractorBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5138980213290646197L;
	
	private OutputCollector _collector = null;
	
	public WordExtractorBolt() {

	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Item item = (Item)input.getValueByField("Item");
		if(item == null)
			return;

		List<String> words = new ArrayList<String>();
		
		for(NamedEntity e : item.getEntities()) {
			words.add(e.getName());
		}
		
		for(String tag : item.getTags()) {
			words.add(tag);
		}
		
		for(String mention : item.getMentions()) {
			words.add(mention);
		}
		
		for(String word : words) {
			_collector.emit(input, new Values(word));
		}
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	
}