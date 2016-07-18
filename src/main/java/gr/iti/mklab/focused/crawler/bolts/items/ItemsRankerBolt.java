package gr.iti.mklab.focused.crawler.bolts.items;

import gr.iti.mklab.framework.common.domain.Item;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class ItemsRankerBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	
	private OutputCollector _collector;
	private String inputField;

	public ItemsRankerBolt(String inputField) {
		this.inputField = inputField;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("Item", "score"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		this._collector = collector;
	}

	public void execute(Tuple tuple) {
		String json = tuple.getStringByField(inputField);
		Item item = Item.toObject(json, Item.class);

		Long shares = item.getShares();
		
		double sharesScore = 1 - Math.exp(-0.05 * shares);
		sharesScore = (sharesScore + 1) / 2;
		
		_collector.emit(new Values(item, sharesScore));
        _collector.ack(tuple);
        
	}   
}