package gr.iti.mklab.focused.crawler.bolts.media;

import gr.iti.mklab.framework.common.domain.MediaItem;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class MediaRankerBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	private OutputCollector _collector;
	private String inputField;

	public MediaRankerBolt(String inputField) {
		this.inputField = inputField;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("MediaItem", "score"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		this._collector = collector;
	}

	public void execute(Tuple tuple) {
		String json = tuple.getStringByField(inputField);
		MediaItem mediaItem = MediaItem.toObject(json, MediaItem.class);

		Long shares = mediaItem.getShares();
		
		double sharesScore = 1 - Math.exp(-0.05 * shares);
		sharesScore = (sharesScore + 1) / 2;
		
		_collector.emit(new Values(mediaItem, sharesScore));
        _collector.ack(tuple);
        
	}   
}