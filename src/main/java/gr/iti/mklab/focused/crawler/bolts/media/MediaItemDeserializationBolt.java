package gr.iti.mklab.focused.crawler.bolts.media;

import static org.apache.storm.utils.Utils.tuple;
import gr.iti.mklab.framework.common.domain.MediaItem;

import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class MediaItemDeserializationBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger _logger;
	
	private OutputCollector _collector;

	private String inputField;

	public MediaItemDeserializationBolt(String inputField) {
		this.inputField = inputField;
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;

		_logger = Logger.getLogger(MediaItemDeserializationBolt.class);
		
	}

	public void execute(Tuple input) {
		try {
			String json = input.getStringByField(inputField);
			MediaItem mediaItem = MediaItem.toObject(json, MediaItem.class);
			if(mediaItem != null) {
				_collector.emit(tuple(mediaItem));
			}
		} catch(Exception e) {
				_logger.error("Exception: "+e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MediaItem"));
	}
	
}
