package gr.iti.mklab.focused.crawler.bolts.webpages;

import gr.iti.mklab.framework.client.search.solr.SolrWebPageHandler;
import gr.iti.mklab.framework.client.search.solr.beans.WebPageBean;
import gr.iti.mklab.framework.common.domain.WebPage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class TextIndexerBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7500656732029697927L;
	
	private Logger _logger;
	
	private String _indexService;
	private SolrWebPageHandler _solrWebPageHandler = null;
	
	private ArrayBlockingQueue<Tuple> _queue;

	private OutputCollector _collector;
	
	public TextIndexerBolt(String indexService) {
		this._indexService = indexService;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		_logger = Logger.getLogger(TextIndexerBolt.class);
		
		_queue = new ArrayBlockingQueue<Tuple>(10000);
		
		_collector = collector;
		
		_logger.info("Connect to " + _indexService);
		_solrWebPageHandler = SolrWebPageHandler.getInstance(_indexService);
		
		Thread t = new Thread(new TextIndexer());
		t.start();
	}

	public void execute(Tuple input) {
		try {
			if(_solrWebPageHandler != null) {
				_queue.add(input);
			}
			else {
				_collector.fail(input);
			}
		}
		catch(Exception ex) {
			_logger.error(ex);
		}
	}

	public class TextIndexer implements Runnable {

		public void run() {
			while(true) {
				try {
					// Just wait 10 seconds
					Thread.sleep(10 * 1000);

					List<Tuple> tuples = new ArrayList<Tuple>();
					_queue.drainTo(tuples);
					
					if(tuples.isEmpty()) {
						_logger.info("There are no web pages to index. Wait some more time.");
						continue;
					}
					
					List<WebPageBean> beans = new ArrayList<WebPageBean>();
					for(Tuple tuple : tuples) {
						try {
							WebPage webPage = (WebPage) tuple.getValueByField("WebPage");
							beans.add(new WebPageBean(webPage));
							_collector.ack(tuple);
						}
						catch(Exception e) {
							_collector.fail(tuple);
						}
						
					}
					
					boolean inserted = _solrWebPageHandler.insert(beans);
					if(inserted) {
						_logger.info(tuples.size() + " web pages indexed in Solr");
					}
					else {
						_logger.error("Indexing in Solr failed for some web pages");
					}
				} catch (Exception e) {
					_logger.error(e);
					continue;
				}
			}
		}
		
	}
}