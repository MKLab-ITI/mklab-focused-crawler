package gr.iti.mklab.focused.crawler.bolts.webpages;

import static org.apache.storm.utils.Utils.tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;


import gr.iti.mklab.focused.crawler.utils.ArticleExtractor;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.WebPage;

public class ArticleExtractionBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2548434425109192911L;
	
	public static String MEDIA_STREAM = "mediaitems";
	public static String WEBPAGE_STREAM = "webpages";
	
	private Logger _logger;
	
	private OutputCollector _collector;
	
	private BlockingQueue<Tuple> _queue;
	
	public ArticleExtractionBolt() {

	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declareStream(MEDIA_STREAM, new Fields("mediaitems"));
    	declarer.declareStream(WEBPAGE_STREAM, new Fields("webpages"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		_logger = Logger.getLogger(ArticleExtractionBolt.class);
		
		_collector = collector;
		
		_queue = new LinkedBlockingQueue<Tuple>();
	    
	    Thread extractorThread = new Thread(new ArticleExtractorProcess(_queue));
	    extractorThread.start();
	    
	}

	public void execute(Tuple input) {
		try {
			_queue.put(input);
			
		} catch (InterruptedException e) {
			_logger.error(e);
		}
	}   
	
	private class ArticleExtractorProcess implements Runnable {

		private BlockingQueue<Tuple> queue;
		
		public ArticleExtractorProcess(BlockingQueue<Tuple> _queue) {
			this.queue = _queue;
		}
		
		public void run() {
			while(true) {
				Tuple input = null;
				try {
					input  = queue.take();
					if(input == null) {
						continue;
					}
					
					WebPage webPage = (WebPage) input.getValueByField("webpages");
					byte[] content = input.getBinaryByField("content");
					
					if(webPage == null || content == null) {
						_collector.fail(input);
						continue;
					}
					
					List<MediaItem> mediaItems = new ArrayList<MediaItem>();
					boolean parsed = ArticleExtractor.parseWebPage(webPage, content, mediaItems);
					if(parsed) { 
						_collector.emit(WEBPAGE_STREAM, input, tuple(webPage));
						for(MediaItem mediaItem : mediaItems) {
							_collector.emit(MEDIA_STREAM, input, tuple(mediaItem));
						}
						_collector.ack(input);
					}
					else {
						_logger.error("Failed to parse " + webPage.getExpandedUrl());
						_collector.fail(input);
					}
					
				} catch (Exception e) {
					_logger.error(e);
					continue;
				}
			}
		}
	}
	

}
