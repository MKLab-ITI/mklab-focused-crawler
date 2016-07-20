package gr.iti.mklab.focused.crawler.bolts.webpages;

import gr.iti.mklab.framework.common.domain.WebPage;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.Map;

import org.apache.log4j.Logger;

import static org.apache.storm.utils.Utils.tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

public class URLExpansionBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5514715036795163046L;
	private static int max_redirects = 4;
	
	private Logger logger;
	private OutputCollector _collector;
	
	private String inputField;
	
	public URLExpansionBolt(String inputField) throws Exception {
		this.inputField = inputField;	
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.logger = Logger.getLogger(URLExpansionBolt.class);	
		this._collector = collector;
	}

	public void execute(Tuple tuple) {	
		WebPage webPage = (WebPage) tuple.getValueByField(inputField);
		if(webPage != null) {
			try {
				String url = webPage.getUrl();
				String expandedUrl = expand(url);
				
				if(expandedUrl != null) {
					try {
						URL temp = new URL(expandedUrl);
						String domain = temp.getHost();
					
						webPage.setExpandedUrl(expandedUrl);
						webPage.setDomain(domain);
						
						synchronized(_collector) {
							_collector.emit(tuple(webPage));
						}
					}
					catch(Exception e) {
						//webPage.setStatus("failed");
						//_collector.emit("update", tuple(webPage));
						logger.error(e);
					}
				}
				else {
					//webPage.setStatus("failed");
					//_collector.emit("update", tuple(webPage));
				}
			} catch (Exception e) {
				//webPage.setStatus("failed");
				//_collector.emit("update", tuple(webPage));
				logger.error(e);
			}
		}
		else {
			Utils.sleep(50);
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(inputField));
	}

	public static String expand(String shortUrl) throws IOException {
		int redirects = 0;
		HttpURLConnection connection;
		while(true && redirects < max_redirects) {
			try {
				URL url = new URL(shortUrl);
				connection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY); 
				connection.setInstanceFollowRedirects(false);
				connection.setReadTimeout(2000);
				connection.connect();
				String expandedURL = connection.getHeaderField("Location");
				if(expandedURL == null) {
					return shortUrl;
				}
				else {
					shortUrl = expandedURL;
					redirects++;
				}    
			}
			catch(Exception e) {
				return null;
			}
		}
		return shortUrl;
    }
}
