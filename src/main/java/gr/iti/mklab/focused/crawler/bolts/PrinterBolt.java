package gr.iti.mklab.focused.crawler.bolts;

import gr.iti.mklab.focused.crawler.bolts.structures.Rankable;
import gr.iti.mklab.focused.crawler.bolts.structures.Rankings;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class PrinterBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9028014271207845500L;
	private OutputCollector collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		collector.ack(input);
		
		Rankings rankings = (Rankings) input.getValue(0);
		for(Rankable r : rankings.getRankings()) {
			System.out.println("MINHASH CLUSTERING: " + r.getObject() + " has " + r.getValue() + " items!");	
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
