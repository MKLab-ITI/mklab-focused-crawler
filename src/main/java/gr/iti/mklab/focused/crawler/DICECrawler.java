package gr.iti.mklab.focused.crawler;

import java.net.UnknownHostException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.solr.config.CountBasedCommit;
import org.apache.storm.solr.config.SolrConfig;
import org.apache.storm.solr.mapper.SolrJsonMapper;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import gr.iti.mklab.focused.crawler.bolts.DeserializationBolt;
import gr.iti.mklab.focused.crawler.bolts.media.MediaExtractionBolt;
import gr.iti.mklab.focused.crawler.bolts.media.MediaTextIndexerBolt;
import gr.iti.mklab.focused.crawler.bolts.webpages.ArticleExtractionBolt;
import gr.iti.mklab.focused.crawler.bolts.webpages.SolrBolt;
import gr.iti.mklab.focused.crawler.bolts.webpages.TextIndexerBolt;
import gr.iti.mklab.focused.crawler.bolts.webpages.URLExpansionBolt;
import gr.iti.mklab.focused.crawler.bolts.webpages.UrlCrawlDeciderBolt;
import gr.iti.mklab.focused.crawler.bolts.webpages.WebPageFetcherBolt;
import gr.iti.mklab.focused.crawler.spouts.RedisSpout;
import gr.iti.mklab.framework.common.domain.WebPage;


public class DICECrawler {
	private static Logger logger = Logger.getLogger(DICECrawler.class);
	
	/**
	 *	@author Manos Schinas - manosetro@iti.gr
	 *
	 *	Entry class for distributed web page processing. 
	 *  This class defines a storm-based pipeline (topology) for the processing of WebPages 
	 *  received from a Redis pub/sub channel. 
	 *  
	 * 	The main steps in the topology are: URLExpansion, ArticleExctraction, MediaExtraction,
	 *  WebPage Text Indexing and Media Text Indexing. 
	 *  
	 *  For more information on Storm distributed processing check this tutorial:
	 *  https://github.com/nathanmarz/storm/wiki/Tutorial
	 *  
	 */
	public static void main(String[] args) throws UnknownHostException {
		
		XMLConfiguration config;
		try {
			if(args.length == 1)
				config = new XMLConfiguration(args[0]);
			else
				config = new XMLConfiguration("./conf/dice.crawler.xml");
		}
		catch(ConfigurationException ex) {
			logger.error(ex);
			return;
		}
	
		StormTopology topology = null;
		try {
			topology = createTopology(config);
		}
		catch(Exception e) {
			logger.error(e);
		}
		
        // Run topology
        String name = config.getString("topology.focusedCrawlerName", "DiceFocusedCrawler");
        boolean local = config.getBoolean("topology.local", true);
        
        Config conf = new Config();
        conf.setDebug(false);
        
        if(!local) {
        	logger.info("Submit topology to Storm cluster");
			try {
				int workers = config.getInt("topology.workers", 20);
				conf.setNumWorkers(workers);
				
				StormSubmitter.submitTopology(name, conf, topology);
			}
			catch(NumberFormatException e) {
				logger.error(e);
			} catch (AlreadyAliveException e) {
				logger.error(e);
			} catch (InvalidTopologyException e) {
				logger.error(e);
			} catch (AuthorizationException e) {
				logger.error(e);
			}
			
		} else {
			logger.info("Run topology in local mode");
			LocalCluster cluster = new LocalCluster();
			conf.setNumWorkers(20);
			cluster.submitTopology(name, conf, topology);
		}
	}
	
	public static StormTopology createTopology(XMLConfiguration config) {
	
		String redisHost = config.getString("redis.hostname", "127.0.0.1");
		int redisPort = config.getInt("redis.port", 6379);
		
		String webPagesChannel = config.getString("redis.webPagesChannel", "webpages");
		
		String indexHostname = config.getString("textindex.host", "xxx.xxx.xxx.xxx");
		String indexPort = config.getString("textindex.port", "8983");
		String webpagesCollection = config.getString("textindex.collections.webpages", "WebPages");
		String mediaitemsCollection = config.getString("textindex.collections.media", "MediaItems");
		
		String indexService = "http://" + indexHostname + ":" + indexPort + "/solr";
		
		BaseRichSpout wpSpout;
		IRichBolt wpDeserializer, urlExpander, urlCrawlDeciderBolt, fetcher, articleExtraction;
		IRichBolt mediaExtraction;
		IRichBolt webpagesSolrUpdater, mediaitemsSolrUpdater;
		
		try {
			// initialize bolts & spouts
			wpSpout = new RedisSpout(redisHost, redisPort, webPagesChannel, "webpages");
			wpDeserializer = new DeserializationBolt<WebPage>("webpages", WebPage.class);
			urlExpander = new URLExpansionBolt("webpages");
			urlCrawlDeciderBolt = new UrlCrawlDeciderBolt("webpages");
			fetcher = new WebPageFetcherBolt("webpages", 6);
			articleExtraction = new ArticleExtractionBolt();
			
			mediaExtraction = new MediaExtractionBolt();

			webpagesSolrUpdater = new SolrBolt(indexService, webpagesCollection, new CountBasedCommit(100));
			mediaitemsSolrUpdater = new SolrBolt(indexService, mediaitemsCollection, new CountBasedCommit(100));
			
		} catch (Exception e) {
			logger.error(e);
			return null;
		}
		
		// Create topology 
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("wpSpout", wpSpout);
		
		// deserializer bolt 
		builder.setBolt("WpDeserializer", wpDeserializer, 2)
			.shuffleGrouping("wpSpout");
		
		// url expander
		builder.setBolt("expander", urlExpander, 4)
			.shuffleGrouping("WpDeserializer");
		
		// url crawl decider 
		builder.setBolt("crawlDecider", urlCrawlDeciderBolt, 1)
			.fieldsGrouping("expander", new Fields("domain"));
		
		// fetching bolt
		builder.setBolt("fetcher", fetcher, 4)
			.fieldsGrouping("crawlDecider", UrlCrawlDeciderBolt.WEBPAGE_STREAM, new Fields("domain"));
		
		// article extraction bolt
		builder.setBolt("articleExtraction", articleExtraction, 8)
			.shuffleGrouping("fetcher");
		
		builder.setBolt("textIndexer", webpagesSolrUpdater, 1)
			.shuffleGrouping("articleExtraction", ArticleExtractionBolt.WEBPAGE_STREAM);
		
		// media item extraction bolt
		builder.setBolt("mediaExtraction", mediaExtraction, 1)
			.shuffleGrouping("crawlDecider", UrlCrawlDeciderBolt.MEDIA_STREAM);
				
		// web pages indexer
		//builder.setBolt("textIndexer", textIndexer, 1)
		//	.shuffleGrouping("articleExtraction", "webpages");

		// media items indexer
		//builder.setBolt("mediatextindexer", mediaTextIndexer, 1)
		//	.shuffleGrouping("articleExtraction", "mediaitems")
		//	.shuffleGrouping("mediaExtraction", "mediaitems");
		
		StormTopology topology = builder.createTopology();
		return topology;
	}
	
}