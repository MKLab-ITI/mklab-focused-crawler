package gr.iti.mklab.focused.crawler;

import java.net.UnknownHostException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import gr.iti.mklab.focused.crawler.bolts.items.EntityExtractionBolt;
import gr.iti.mklab.focused.crawler.bolts.items.EventDetectionBolt;
import gr.iti.mklab.focused.crawler.bolts.items.ItemDeserializationBolt;
import gr.iti.mklab.focused.crawler.bolts.items.PosTaggingBolt;
import gr.iti.mklab.focused.crawler.bolts.items.TokenizationBolt;
import gr.iti.mklab.focused.crawler.spouts.RedisSpout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;

public class SocialMediaMonitor {

	private static Logger logger = Logger.getLogger(SocialMediaMonitor.class);
	
	/**
	 *	@author Manos Schinas - manosetro@iti.gr
	 */
	public static void main(String[] args) throws UnknownHostException {
		
		XMLConfiguration config;
		try {
			if(args.length == 1)
				config = new XMLConfiguration(args[0]);
			else
				config = new XMLConfiguration("./conf/focused.crawler.xml");
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
        String name = config.getString("topology.focusedCrawlerName", "FocusedCrawler");
        boolean local = config.getBoolean("topology.local", true);
        
        Config conf = new Config();
        conf.setDebug(false);
        
        if(!local) {
        	System.out.println("Submit topology to Storm cluster");
			try {
				int workers = config.getInt("topology.workers", 4);
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
			cluster.submitTopology(name, conf, topology);
		}
	}
	
	public static StormTopology createTopology(XMLConfiguration config) {
	
		String redisHost = config.getString("redis.hostname", "localhost");
		int redisPort = config.getInt("redis.port", 6379);
		
		String itemsChannel = config.getString("redis.itemsChannel", "items");
		
		String nerModel = config.getString("ner.model", "english.all.3class.distsim.crf.ser.gz");
		String POSModel = config.getString("pos.model", "english-left3words-distsim.tagger");
		
		BaseRichSpout itemsSpout;
		IRichBolt entityExtractor, posTagger;
		IRichBolt itemDeserializer, tokenizer, eventDetector;	
		try {
			itemsSpout = new RedisSpout(redisHost, redisPort, itemsChannel);
			itemDeserializer = new ItemDeserializationBolt(itemsChannel);

			entityExtractor = new EntityExtractionBolt(nerModel);
			posTagger = new PosTaggingBolt(POSModel);
			
			tokenizer = new TokenizationBolt(TokenizationBolt.TokenType.NE);
			
			eventDetector = new EventDetectionBolt(5, 60);
			
		} catch (Exception e) {
			logger.error(e);
			return null;
		}
		
		// Create topology 
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("itemsSpout", itemsSpout, 1);
				
		builder.setBolt("itemDeserializer", itemDeserializer, 4).shuffleGrouping("itemsSpout");
		builder.setBolt("entityExtractor", entityExtractor, 4).shuffleGrouping("itemDeserializer");
		builder.setBolt("posTagger", posTagger, 4).shuffleGrouping("entityExtractor");
		builder.setBolt("tokenizer", tokenizer, 8).shuffleGrouping("entityExtractor");
		
		builder.setBolt("eventDetector", eventDetector, 1).shuffleGrouping("tokenizer");
		
		StormTopology topology = builder.createTopology();
		return topology;
	}
}