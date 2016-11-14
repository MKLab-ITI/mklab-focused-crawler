package gr.iti.mklab.focused.crawler;

import gr.iti.mklab.focused.crawler.bolts.SolrBolt;
import gr.iti.mklab.focused.crawler.bolts.items.EntityExtractionBolt;
import gr.iti.mklab.focused.crawler.bolts.items.IntermediateRankingsBolt;
import gr.iti.mklab.focused.crawler.bolts.items.LabelerBolt;
import gr.iti.mklab.focused.crawler.bolts.items.MinHashExtractorBolt;
import gr.iti.mklab.focused.crawler.bolts.items.MongoDBWriter;
import gr.iti.mklab.focused.crawler.bolts.items.TermsExtractorBolt;
import gr.iti.mklab.focused.crawler.bolts.items.TermsRollingCountBolt;
import gr.iti.mklab.focused.crawler.bolts.items.TotalRankingsBolt;
import gr.iti.mklab.focused.crawler.spouts.TwitterSampleSpout;
import gr.iti.mklab.framework.client.search.solr.beans.ItemBean;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.solr.config.CountBasedCommit;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class TrendingTopicsDetector {

	private static final int WINDOW_LENGTH = 6 * 5 * 60;
	private static final int EMIT_FREQUENCY = 5 * 60;
    private static final int TOP_N = 20;


	
    private static StormTopology createTopology(XMLConfiguration config) throws InterruptedException {
    	
        String spoutId = "itemsInjector";
        //String itemDeserializerId = "itemDeserializer";
        String entityExtractionId = "entityExtraction";
        String minhashExtractorId = "minhashExtractor";
        String wordExtractorId = "wordExtractor";
        String termsCounterId = "termsCounter";
        String minhashCounterId = "minhashCounter";
        String intermediateRankerId = "intermediateRanker";
        String totalRankerId = "finalRanker";
        String minhashRankerId = "minhashRanker";
        String topicsMongoDBWriterID = "topicsMongoDBWriter";
        String clustersMongoDBWriterID = "clustersMongoDBWriter";
        String solrUpdaterId = "solrUpdater";
        String topicsLabelerID = "topicsLabeler";
        String clustersLabelerID = "clustersLabeler";
        
        //String redisHost = config.getString("redis.hostname", "127.0.0.1");
		//int redisPort = config.getInt("redis.port", 6379);		
		//String redisChannel = config.getString("redis.itemsChannel", "items");
		
		String consumerKey = config.getString("twitter.consumerKey", "");
		String consumerSecret = config.getString("twitter.consumerSecret", "");
		String accessToken = config.getString("twitter.accessToken", "");
		String accessTokenSecret = config.getString("twitter.accessTokenSecret", "");
		
		String mongodbHost = config.getString("mongodb.hostname", "160.40.50.207");
		String mongodbDatabase = config.getString("mongodb.db", "dice");		
		String topicsCollection = config.getString("mongodb.collection", "topics");
		String clustersCollection = config.getString("mongodb.clusters", "clusters");
		
		String indexHostname = config.getString("textindex.host", "xxx.xxx.xxx.xxx");
		String indexPort = config.getString("textindex.port", "8983");
		String itemsCollection = config.getString("textindex.collections.items", "Items");
		String indexService = "http://" + indexHostname + ":" + indexPort + "/solr";
		
        TopologyBuilder builder = new TopologyBuilder();
        
        //builder.setSpout(spoutId, new RedisSpout(redisHost, redisPort, redisChannel, "Item"));
        //builder.setBolt(itemDeserializerId, new DeserializationBolt<Item>("Item", Item.class), 4).shuffleGrouping(spoutId);
        
        builder.setSpout(spoutId, new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret));
        
        builder.setBolt(entityExtractionId, new EntityExtractionBolt(), 4).shuffleGrouping(spoutId);
        builder.setBolt(minhashExtractorId, new MinHashExtractorBolt(32), 4).shuffleGrouping(entityExtractionId);
        builder.setBolt(wordExtractorId, new TermsExtractorBolt(), 2).shuffleGrouping(minhashExtractorId);
        
        // solr indexing
        builder.setBolt(solrUpdaterId, new SolrBolt(indexService, itemsCollection, new CountBasedCommit(100), ItemBean.class, "Item")).shuffleGrouping(minhashExtractorId);
        
        // topic detection bolts
        builder.setBolt(termsCounterId, new TermsRollingCountBolt(WINDOW_LENGTH, EMIT_FREQUENCY), 4).fieldsGrouping(wordExtractorId, "terms", new Fields("term"));
        builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N, EMIT_FREQUENCY), 4).fieldsGrouping(termsCounterId, new Fields("obj"));
        builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N, EMIT_FREQUENCY), 1).globalGrouping(intermediateRankerId);
        builder.setBolt(topicsMongoDBWriterID, new MongoDBWriter(mongodbHost, mongodbDatabase, topicsCollection), 1).shuffleGrouping(totalRankerId);
        builder.setBolt(topicsLabelerID, new LabelerBolt(mongodbHost, mongodbDatabase, topicsCollection, indexService, itemsCollection, "title"), 1).shuffleGrouping(topicsMongoDBWriterID);
        
        // minhash clustering
        builder.setBolt(minhashCounterId, new TermsRollingCountBolt(WINDOW_LENGTH, EMIT_FREQUENCY), 4).fieldsGrouping(wordExtractorId, "minhash", new Fields("minhash"));
        builder.setBolt(minhashRankerId, new IntermediateRankingsBolt(TOP_N, EMIT_FREQUENCY), 1).fieldsGrouping(minhashCounterId, new Fields("obj"));
        builder.setBolt(clustersMongoDBWriterID, new MongoDBWriter(mongodbHost, mongodbDatabase, clustersCollection), 1).shuffleGrouping(minhashRankerId);
        builder.setBolt(clustersLabelerID, new LabelerBolt(mongodbHost, mongodbDatabase, clustersCollection, indexService, itemsCollection, "minhash"), 1).shuffleGrouping(clustersMongoDBWriterID);
        
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
    	
        String topologyName = args.length == 1 ? args[0] : "DiceTopicDetection";
        
    	XMLConfiguration config;
		try {
				ClassLoader classLoader = TrendingTopicsDetector.class.getClassLoader();
				config = new XMLConfiguration();
                                config.load(classLoader.getResourceAsStream("dice.topic-detector.xml"));
		}
		catch(ConfigurationException ex) {
			ex.printStackTrace();
			return;
		}
		
    	Config topologyConfig = new Config();
    	topologyConfig.setDebug(false);
    	Config.setMessageTimeoutSecs(topologyConfig, WINDOW_LENGTH);
    	
        StormTopology topology = createTopology(config);
        
        Boolean local = config.getBoolean("topology.local", false);
    	if(!local) {
			try {
				StormSubmitter.submitTopology(topologyName, topologyConfig, topology);
			}
			catch(NumberFormatException | AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
				e.printStackTrace();
			}
			
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, topologyConfig, topology);
		}
    	
    }
}