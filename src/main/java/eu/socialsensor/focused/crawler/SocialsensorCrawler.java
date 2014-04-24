package eu.socialsensor.focused.crawler;

import java.net.UnknownHostException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import eu.socialsensor.focused.crawler.bolts.media.ClustererBolt;
import eu.socialsensor.focused.crawler.bolts.media.ConceptDetectionBolt;
import eu.socialsensor.focused.crawler.bolts.media.MediaRankerBolt;
import eu.socialsensor.focused.crawler.bolts.media.MediaTextIndexerBolt;
import eu.socialsensor.focused.crawler.bolts.media.MediaUpdaterBolt;
import eu.socialsensor.focused.crawler.bolts.media.VisualIndexerBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.ArticleExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.MediaExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.RankerBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.RedisBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.TextIndexerBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.URLExpanderBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.WebPagesUpdaterBolt;
import eu.socialsensor.focused.crawler.spouts.RedisSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;

public class SocialsensorCrawler {

	/**
	 *	@author Manos Schinas - manosetro@iti.gr
	 *
	 *	Entry class for SocialSensor focused crawling.  
	 *  This class defines a storm-based pipeline (topology) for the processing of MediaItems and WebPages
	 *  collected by StreamManager.
	 *  
	 *  For more information on Storm distributed processing check this tutorial:
	 *  https://github.com/nathanmarz/storm/wiki/Tutorial
	 *  
	 */
	public static void main(String[] args) throws UnknownHostException {
		
		Logger logger = Logger.getLogger(SocialsensorCrawler.class);
		
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
		
		String redisHost = config.getString("redis.hostname", "xxx.xxx.xxx.xxx");
		
		String mongodbHostname = config.getString("mongodb.hostname", "xxx.xxx.xxx.xxx");
		String mediaItemsDB = config.getString("mongodb.mediaItemsDB", "Prototype");
		String mediaItemsCollection = config.getString("mongodb.mediaItemsCollection", "MediaItems_WP");
		String streamUsersDB = config.getString("mongodb.streamUsersDB", "Prototype");
		String streamUsersCollection = config.getString("mongodb.streamUsersCollection", "StreamUsers");
		String webPagesDB = config.getString("mongodb.webPagesDB", "Prototype");
		String webPagesCollection = config.getString("mongodb.webPagesCollection", "WebPages");
		String clustersDB = config.getString("mongodb.clustersDB", "Prototype");
		String clustersCollection = config.getString("mongodb.clustersCollection", "MediaClusters");
		
		

		String visualIndexHostname = config.getString("visualindex.hostname");
		String visualIndexCollection = config.getString("visualindex.collection");
		
		String learningFiles = config.getString("visualindex.learningfiles");
		if(!learningFiles.endsWith("/"))
			learningFiles = learningFiles + "/";
		
		String[] codebookFiles = { 
				learningFiles + "surf_l2_128c_0.csv",
				learningFiles + "surf_l2_128c_1.csv", 
				learningFiles + "surf_l2_128c_2.csv",
				learningFiles + "surf_l2_128c_3.csv" };
		
		String pcaFile = learningFiles + "pca_surf_4x128_32768to1024.txt";
		
		String textIndexHostname = config.getString("textindex.host", "http://xxx.xxx.xxx.xxx:8080/solr");
		String textIndexCollection = config.getString("textindex.collections.webpages", "WebPages");
		String textIndexService = textIndexHostname + "/" + textIndexCollection;
		
		String mediaTextIndexCollection = config.getString("textindex.collections.media", "MediaItems");
		String mediaTextIndexService = textIndexHostname + "/" + mediaTextIndexCollection;
		
		String conceptDetectorMatlabfile = config.getString("conceptdetector.matlabfile");
		
		BaseRichSpout wpSpout, miSpout;
		IRichBolt wpRanker, miRanker;
		IRichBolt urlExpander, articleExtraction, mediaExtraction;
		IRichBolt mediaUpdater, miEmitter, updater, textIndexer;
		IRichBolt visualIndexer, clusterer, mediaTextIndexer, conceptDetector;
		
		try {
			wpSpout = new RedisSpout(redisHost, "webpages", "url");
			miSpout = new RedisSpout(redisHost, "media-temp", "id");
			
			wpRanker = new RankerBolt("webpages");
			miRanker = new MediaRankerBolt("media-temp");
			
			urlExpander = new URLExpanderBolt("webpages");
			
			articleExtraction = new ArticleExtractionBolt(48);
			mediaExtraction = new MediaExtractionBolt();
			updater = new WebPagesUpdaterBolt(mongodbHostname, webPagesDB, webPagesCollection);
			textIndexer = new TextIndexerBolt(textIndexService);
			
			miEmitter = new RedisBolt(redisHost, "media-temp");
					
			visualIndexer = new VisualIndexerBolt(visualIndexHostname, visualIndexCollection, codebookFiles, pcaFile);
			clusterer = new ClustererBolt(mongodbHostname, mediaItemsDB, mediaItemsCollection, clustersDB, clustersCollection,
					visualIndexHostname, visualIndexCollection);
			conceptDetector = new ConceptDetectionBolt(conceptDetectorMatlabfile);
			
			mediaUpdater = new MediaUpdaterBolt(mongodbHostname, mediaItemsDB, mediaItemsCollection, streamUsersDB, streamUsersCollection);
			mediaTextIndexer = new MediaTextIndexerBolt(mediaTextIndexService);
			
		} catch (Exception e) {
			logger.error(e);
			return;
		}
		
		// Create topology 
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("wpInjector", wpSpout, 1);
		builder.setSpout("miInjector", miSpout, 1);
		
		builder.setBolt("wpRanker", wpRanker, 4).shuffleGrouping("wpInjector");
		builder.setBolt("miRanker", miRanker, 4).shuffleGrouping("miInjector");
		
		builder.setBolt("expander", urlExpander, 8).shuffleGrouping("wpRanker");
		builder.setBolt("articleExtraction", articleExtraction, 1).shuffleGrouping("expander", "webpage");
		builder.setBolt("mediaExtraction", mediaExtraction, 4).shuffleGrouping("expander", "media");
		builder.setBolt("updater", updater, 4)
			.shuffleGrouping("articleExtraction", "webpage")
			.shuffleGrouping("mediaExtraction", "webpage");
		builder.setBolt("textIndexer", textIndexer, 1)
			.shuffleGrouping("articleExtraction", "webpage");
		
		builder.setBolt("miEmitter", miEmitter, 1).shuffleGrouping("mediaupdater");

        builder.setBolt("indexer", visualIndexer, 16)
			.shuffleGrouping("articleExtraction", "media")
			.shuffleGrouping("mediaExtraction", "media");
		
        builder.setBolt("conceptDetector", conceptDetector, 1).shuffleGrouping("indexer");
        
        builder.setBolt("clusterer", clusterer, 1).shuffleGrouping("indexer");   
		
        builder.setBolt("mediaupdater", mediaUpdater, 1).shuffleGrouping("conceptDetector");
		builder.setBolt("mediaTextIndexer", mediaTextIndexer, 1).shuffleGrouping("conceptDetector");
		
        // Run topology
        String name = "SocialsensorCrawler";
        boolean local = config.getBoolean("topology.local", true);
        
        Config conf = new Config();
        conf.setDebug(false);
        
        if(!local) {
        	logger.info("Submit topology to Storm cluster");
			try {
				int workers = config.getInt("topology.workers", 2);
				conf.setNumWorkers(workers);
				
				StormSubmitter.submitTopology(name, conf, builder.createTopology());
			}
			catch(NumberFormatException e) {
				logger.error(e);
			} catch (AlreadyAliveException e) {
				logger.error(e);
			} catch (InvalidTopologyException e) {
				logger.error(e);
			}
			
		} else {
			logger.info("Run topology in local mode");
			try {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(name, conf, builder.createTopology());
			}
			catch(Exception e) {
				logger.error(e);
			}
		}
	}
}