package gr.iti.mklab.focused.crawler.bolts.items;

import static org.apache.storm.utils.Utils.tuple;
import gr.iti.mklab.framework.common.domain.Item;

import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class ItemDeserializationBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger _logger;
	
	private OutputCollector _collector;

	private String inputField;

	public ItemDeserializationBolt(String inputField) {
		this.inputField = inputField;
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		_logger = Logger.getLogger(ItemDeserializationBolt.class);
	}

	public void execute(Tuple input) {
		try {
			String json = input.getStringByField(inputField);
			if(json != null) {
				Item item = Item.toObject(json, Item.class);
				_collector.emit(tuple(item));
			}
		} catch(Exception e) {
			_logger.error("Exception: "+e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Item"));
	}

	/*
	private class DeserializerThread extends Thread {

		Queue<String> queue;
		public DeserializerThread(Queue<String> queue) {	
			this.queue = queue;
		}

		public void run() {
			while(true) {
				try {
					String json = null;
					synchronized(_queue) {
						json = queue.poll();
					}
					
					if(json == null) {
						Utils.sleep(500);
					}
					else {
						Item item = ItemFactory.create(json);
						_collector.emit(tuple(item));
					}
				}
				catch(Exception e) {
					Utils.sleep(500);
					continue;
				}
			}
		};
	}
	*/
	
}
