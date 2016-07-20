package gr.iti.mklab.focused.crawler.bolts.webpages;

import static org.apache.storm.utils.Utils.tuple;
import gr.iti.mklab.framework.common.domain.WebPage;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

public class WebPageFetcherBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2548434425109192911L;
	
	private Logger _logger;
	
	private OutputCollector _collector;
	private HttpClient _httpclient;
	
	private PoolingHttpClientConnectionManager _cm;
	
	private int numOfFetchers = 24;
	
	private BlockingQueue<WebPage> _queue;
	private BlockingQueue<List<Object>> _tupleQueue;

	private RequestConfig _requestConfig;

	private long receivedTuples = 0;
	
	private Thread _emitter;
	private List<Thread> _fetchers;

	private String inputField;
	
	public WebPageFetcherBolt(String inputField) {
		this.inputField = inputField;
	}
	
	public WebPageFetcherBolt(String inputField, int numOfFetchers) {
		this.inputField = inputField;
		this.numOfFetchers = numOfFetchers;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {    	
    	declarer.declare(new Fields(inputField, "content"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		_logger = Logger.getLogger(WebPageFetcherBolt.class);
		
		_collector = collector;
		
		_queue = new LinkedBlockingQueue<WebPage>();
		_tupleQueue =  new LinkedBlockingQueue<List<Object>>();
		
		_cm = new PoolingHttpClientConnectionManager();
		_cm.setMaxTotal(numOfFetchers);
		_cm.setDefaultMaxPerRoute(10);

		_httpclient = HttpClients.custom()
		        .setConnectionManager(_cm)
		        .build();
		
		// Set timeout parameters for Http requests
		_requestConfig = RequestConfig.custom()
		        .setSocketTimeout(30000)
		        .setConnectTimeout(30000)
		        .build();
	    
	    _emitter = new Thread(new Emitter(_collector, _tupleQueue));
	    _emitter.start();
	    
	    _fetchers = new ArrayList<Thread>(numOfFetchers);
	    for(int i=0;i<numOfFetchers; i++) {
	    	Thread fetcher = new Thread(new HttpFetcher(_queue));
	    	fetcher.start();
	    	
	    	_fetchers.add(fetcher);
	    }
	}

	public void execute(Tuple tuple) {
		receivedTuples++;
		WebPage webPage = (WebPage) tuple.getValueByField(inputField);
		try {
			if(webPage != null) {
				_queue.put(webPage);
			}
		} catch (InterruptedException e) {
			_logger.error(e);
		}
	}   
	
	private class Emitter implements Runnable {

		private OutputCollector _collector;
		private BlockingQueue<List<Object>> _tupleQueue;
		
		private int mediaTuples = 0, webPagesTuples = 0;
		
		public Emitter(OutputCollector collector, BlockingQueue<List<Object>> tupleQueue) {
			_collector = collector;
			_tupleQueue = tupleQueue;
		}
		
		public void run() {
			while(true) {
				List<Object> tuple = _tupleQueue.poll();
				if(tuple != null) {
					synchronized(_collector) {
						_collector.emit(tuple);
					}
				}
				else {
					Utils.sleep(500);
				}
				
				if((mediaTuples%100==0 || webPagesTuples%100==0) && (mediaTuples!=0 || webPagesTuples!=0)) {
					_logger.info(receivedTuples + " tuples received, " + mediaTuples + " media tuples emmited, " + 
							webPagesTuples + " web page tuples emmited");
					_logger.info(getWorkingFetchers() + " fetchers out of " + numOfFetchers + " are working.");
				}
			}
		}
	}
	
	private int getWorkingFetchers() {
		int working = 0;
		for(Thread fetcher : _fetchers) {
			if(fetcher.isAlive()) {
				working++;
			}
		}
		return working;
	}
	
	private class HttpFetcher implements Runnable {

		private BlockingQueue<WebPage> queue;
		
		public HttpFetcher(BlockingQueue<WebPage> _queue) {
			this.queue = _queue;
		}
		
		public void run() {
			while(true) {
				
				WebPage webPage = null;
				try {
					webPage = queue.take();
				} catch (Exception e) {
					_logger.error(e);
					continue;
				}
				
				if(webPage == null) {
					continue;
				}
				
				String expandedUrl = webPage.getExpandedUrl();
				if(expandedUrl == null || expandedUrl.length() > 300) {
					_tupleQueue.add(tuple(webPage, new byte[0]));
					continue;
				}
				
				HttpGet httpget = null;
				try {
					
					URI uri = new URI(expandedUrl.replaceAll(" ", "%20").replaceAll("\\|", "%7C"));
					
					httpget = new HttpGet(uri);
					httpget.setConfig(_requestConfig);
					HttpResponse response = _httpclient.execute(httpget);
					
					HttpEntity entity = response.getEntity();
					ContentType contentType = ContentType.get(entity);
	
					if(!contentType.getMimeType().equals(ContentType.TEXT_HTML.getMimeType())) {
						_logger.error("URL: " + webPage.getExpandedUrl() + "   Not supported mime type: " + contentType.getMimeType());
						_tupleQueue.add(tuple(webPage, new byte[0]));
						
						continue;
					}
					
					InputStream input = entity.getContent();
					byte[] content = IOUtils.toByteArray(input);
					
					List<Object> tuple = tuple(webPage, content);
					_logger.info("URL: " + webPage.getExpandedUrl() + " Content: " + content.length + " bytes");
					_tupleQueue.add(tuple);
					
				} catch (Exception e) {
					_logger.error("for " + expandedUrl, e);
					_tupleQueue.add(tuple(webPage, new byte[0]));
				}
				finally {
					if(httpget != null) {
						httpget.abort();
					}
				}
				
			}
		}
	}
	
}
