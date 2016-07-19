package gr.iti.mklab.focused.crawler.bolts.webpages;

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

public class WebPageDeserializationBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger _logger;
	
	private OutputCollector _collector;

	private String inputField;

	public WebPageDeserializationBolt(String inputField) {
		this.inputField = inputField;
		JSONable.mapClass(WebPage.class);
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;	
		_logger = Logger.getLogger(WebPageDeserializationBolt.class);
	}

	public void execute(Tuple input) {
		try {
			String json = input.getStringByField(inputField);
			WebPage webPage = WebPage.toObject(json, WebPage.class);
			if(webPage != null) {
				_collector.emit(tuple(webPage));
			}
		} catch(Exception e) {
			e.printStackTrace();
			_logger.error("Exception: "+e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(inputField));
	}

}
