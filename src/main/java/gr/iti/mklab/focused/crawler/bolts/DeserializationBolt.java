package gr.iti.mklab.focused.crawler.bolts;

import static org.apache.storm.utils.Utils.tuple;
import gr.iti.mklab.framework.common.domain.JSONable;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import com.mongodb.DBObject;

public class DeserializationBolt<K> extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger _logger;
	
	private OutputCollector _collector;

	private String inputField;
	private Class<K> c;

	
	private long receivedTuples = 0, emmited = 0;
	
	public DeserializationBolt(String inputField, Class<K> c) {
		this.inputField = inputField;
		this.c = c;
		JSONable.mapClass(c);
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;	
		_logger = Logger.getLogger(DeserializationBolt.class);
		JSONable.mapClass(c);
	}

	public void execute(Tuple input) {
		try {
			receivedTuples++;
			DBObject obj = (DBObject) input.getValueByField(inputField);
			K object = JSONable.toObject(obj, c);
			if(object != null) {
				emmited++;
				_collector.emit(input, tuple(object));
				_collector.ack(input);
			}
			else {
				_logger.error("Failed to deserialize " + obj);
				_collector.fail(input);
			}
			
			if(receivedTuples%1000==0) {
				_logger.info(receivedTuples + " tuples received, " + emmited + " emmited.");
			}
			
		} catch(Exception e) {
			_collector.fail(input);
			_logger.error("Exception: " + e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(inputField));
	}

}
