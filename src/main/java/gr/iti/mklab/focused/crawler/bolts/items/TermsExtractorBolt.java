package gr.iti.mklab.focused.crawler.bolts.items;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.NamedEntity;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TermsExtractorBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5138980213290646197L;
	
	private OutputCollector _collector = null;
	
	public TermsExtractorBolt() {

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

		List<Pair<String, String>> terms = new ArrayList<Pair<String, String>>();
		
		for(NamedEntity e : item.getEntities()) {
			terms.add(Pair.of(e.getName().toLowerCase(), "entity"));
		}
		
		for(String tag : item.getTags()) {
			terms.add(Pair.of(tag.toLowerCase(), "tag"));
		}

		for(Pair<String, String> term : terms) {
			if(term.getKey() != null) {
				_collector.emit("terms", input, new Values(term.getKey(), term.getValue()));
			}
		}
		
		if(item.getMinhash() != null && item.getMinhash().length() > 0) {
			_collector.emit("minhash", input, new Values(item.getMinhash()));
		}
		
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("terms", new Fields("term", "type"));
		declarer.declareStream("minhash", new Fields("minhash"));
	}

	
}