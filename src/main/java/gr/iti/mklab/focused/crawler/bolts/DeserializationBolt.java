package gr.iti.mklab.focused.crawler.bolts;

import static org.apache.storm.utils.Utils.tuple;
import gr.iti.mklab.framework.common.domain.JSONable;
import gr.iti.mklab.framework.common.domain.WebPage;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class DeserializationBolt<K> extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger _logger;
	
	private OutputCollector _collector;

	private String inputField;
	private Class<K> c;

	public DeserializationBolt(String inputField, Class<K> c) {
		this.inputField = inputField;
		this.c = c;
		JSONable.mapClass(c);
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;	
		_logger = Logger.getLogger(DeserializationBolt.class);
	}

	public void execute(Tuple input) {
		try {
			String json = input.getStringByField(inputField);
			K object = WebPage.toObject(json, c);
			if(object != null) {
				_collector.emit(input, tuple(object));
				_collector.ack(input);
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
