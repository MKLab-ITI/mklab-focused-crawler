package gr.iti.mklab.focused.crawler.bolts.webpages;

import static org.apache.storm.utils.Utils.tuple;
import gr.iti.mklab.framework.common.domain.WebPage;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
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
	
	private long receivedTuples = 0, emmited = 0;
	
	private BlockingQueue<Tuple> _queue;

	private RequestConfig _requestConfig;
	
	private Thread _statusWriter;
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
		
		_queue = new LinkedBlockingQueue<Tuple>();
		
		_cm = new PoolingHttpClientConnectionManager();
		
		_cm.setMaxTotal(8*numOfFetchers);
		_cm.setDefaultMaxPerRoute(numOfFetchers);
		
		_httpclient = HttpClients.custom()
		        .setConnectionManager(_cm)
		        .build();
		
		// Set timeout parameters for Http requests
		_requestConfig = RequestConfig.custom()
		        .setSocketTimeout(5000)
		        .setConnectTimeout(5000)
		        .setCookieSpec(CookieSpecs.STANDARD)
		        .build();
	    
		_statusWriter = new Thread(new StatusWriter());
		_statusWriter.start();
	    
	    _fetchers = new ArrayList<Thread>(numOfFetchers);
	    for(int i=0;i<numOfFetchers; i++) {
	    	Thread fetcher = new Thread(new HttpFetcher(_queue));
	    	fetcher.start();
	    	
	    	_fetchers.add(fetcher);
	    }
	}


	public void execute(Tuple input) {
		try {
			receivedTuples++;
			_queue.put(input);
		} catch (InterruptedException e) {
			_collector.fail(input);
			_logger.error(e);
		}
	}   
	

	private class StatusWriter implements Runnable {
		public void run() {
			while(true) {
				Utils.sleep(5*60000l);
				_logger.info("Fetcher: " + receivedTuples + " tuples received, " + emmited + " emmited. " + getWorkingFetchers() + " fetchers out of " + numOfFetchers + " are working.");
			}
		}
	}
	
	public int getWorkingFetchers() {
		int working = 0;
		for(Thread fetcher : _fetchers) {
			if(fetcher.isAlive()) {
				working++;
			}
		}
		return working;
	}
	
	
	private class HttpFetcher implements Runnable {

		private BlockingQueue<Tuple> queue;
		
		public HttpFetcher(BlockingQueue<Tuple> _queue) {
			this.queue = _queue;
		}
		
		public void run() {
			while(true) {
				
				Tuple input = null;
				try {
					input = queue.take();
					if(input == null) {
						continue;
					}
				}
				catch(InterruptedException e) {
					continue;
				}
				
				WebPage webPage = null;
				try {	
					webPage = (WebPage) input.getValueByField(inputField);
					if(webPage == null) {
						_collector.fail(input);
						continue;
					}
				} catch (Exception e) {
					_logger.error(e);
					_collector.fail(input);
					continue;
				}
				
				String expandedUrl = webPage.getExpandedUrl();
				if(expandedUrl == null || expandedUrl.length() > 300) {
					_collector.ack(input);
					continue;
				}
				
				HttpGet httpget = null;
				try {
					
					URI uri = new URI(expandedUrl.replaceAll(" ", "%20").replaceAll("\\|", "%7C"));
					
					httpget = new HttpGet(uri);
					httpget.setConfig(_requestConfig);
					
					HttpResponse response = _httpclient.execute(httpget);
					
					HttpEntity entity = response.getEntity();
					if(entity == null) {
						_logger.error("URL: " + webPage.getExpandedUrl() + "   Fetcher got empty entity in repsonse");
						_collector.ack(input);
					}
					
					ContentType contentType = ContentType.get(entity);
					if(!contentType.getMimeType().equals(ContentType.TEXT_HTML.getMimeType())) {
						_logger.error("URL: " + webPage.getExpandedUrl() + "   Not supported mime type: " + contentType.getMimeType());

						EntityUtils.consumeQuietly(entity);
						_collector.ack(input);
						
						continue;
					}
					
					byte[] content = EntityUtils.toByteArray(entity);
					
					List<Object> outputTuple = tuple(webPage, content);
					
					emmited++;
					_collector.emit(input, outputTuple);
					_collector.ack(input);
					
				} catch (Exception e) {
					_logger.error("Exception for " + expandedUrl + ": " + e.getMessage());
					_collector.ack(input);
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
