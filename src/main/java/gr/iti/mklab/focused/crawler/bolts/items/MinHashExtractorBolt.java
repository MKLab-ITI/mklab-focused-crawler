package gr.iti.mklab.focused.crawler.bolts.items;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import gr.iti.mklab.focused.crawler.utils.MinHash;
import gr.iti.mklab.focused.crawler.utils.TextUtils;
import gr.iti.mklab.framework.common.domain.Item;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MinHashExtractorBolt extends BaseRichBolt {

	private static final long serialVersionUID = 7935961067953158062L;

	private OutputCollector _collector;
	private Logger _logger;

	private MinHash minHash;

	private int length;
		
	public MinHashExtractorBolt(int length) {
		this.length = length;
	}
	
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {       
		
		this._collector = collector;
		this._logger = Logger.getLogger(MinHashExtractorBolt.class);
	   
		this.minHash = MinHash.getInstance(1, length);
	}

	public void execute(Tuple input) {
		Item item = (Item)input.getValueByField("Item");
		try {
			if(item == null) {
				return;
			}
			
			String title = item.getTitle();

			title = TextUtils.clean(title);
			title = title.toLowerCase();
			title = TextUtils.normalize(title);
			
			List<String> tokens = TextUtils.tokenize(title);
			TextUtils.cleanTokens(tokens);
			
			title = StringUtils.join(tokens, " ");
			
			if(tokens.isEmpty() || title.equals("")) {
				_collector.ack(input);
				return;
			}
			
			byte[] hashdata = minHash.calculate(title);
			String minhash = MinHash.toString(hashdata);
			
			item.setMinhash(minhash);
		}
		catch(Exception e) {
			e.printStackTrace();
			_logger.error(e);
		}
		
		_collector.emit(input, new Values(item));
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Item"));
	}
	
}