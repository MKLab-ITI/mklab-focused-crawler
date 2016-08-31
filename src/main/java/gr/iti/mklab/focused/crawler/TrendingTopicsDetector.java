package gr.iti.mklab.focused.crawler;

import java.io.File;

import gr.iti.mklab.focused.crawler.bolts.DeserializationBolt;
import gr.iti.mklab.focused.crawler.bolts.items.EntityExtractionBolt;
import gr.iti.mklab.focused.crawler.bolts.items.IntermediateRankingsBolt;
import gr.iti.mklab.focused.crawler.bolts.items.RollingCountBolt;
import gr.iti.mklab.focused.crawler.bolts.items.TotalRankingsBolt;
import gr.iti.mklab.focused.crawler.bolts.items.WordExtractorBolt;
import gr.iti.mklab.focused.crawler.spouts.RedisSpout;
import gr.iti.mklab.framework.common.domain.Item;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class TrendingTopicsDetector {

    private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
    private static final int TOP_N = 20;

    private static StormTopology createTopology(XMLConfiguration config) throws InterruptedException {
    	
        String spoutId = "itemsInjector";
        String itemDeserializerId = "itemDeserializer";
        String entityExtractionId = "entityExtraction";
        String wordExtractorId = "wordExtractor";
        String counterId = "counter";
        String intermediateRankerId = "intermediateRanker";
        String totalRankerId = "finalRanker";

        String redisHost = config.getString("redis.hostname", "127.0.0.1");
		int redisPort = config.getInt("redis.port", 6379);		
		String redisChannel = config.getString("redis.itemsChannel", "items");
		
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(spoutId, new RedisSpout(redisHost, redisPort, redisChannel, "items"));
        
        builder.setBolt(itemDeserializerId, new DeserializationBolt<Item>("items", Item.class), 4).shuffleGrouping(spoutId);
        builder.setBolt(entityExtractionId, new EntityExtractionBolt(""), 4).shuffleGrouping(itemDeserializerId);
        builder.setBolt(wordExtractorId, new WordExtractorBolt(), 2).shuffleGrouping(entityExtractionId);
        
        // topic detection bolts
        builder.setBolt(counterId, new RollingCountBolt(9, 3), 4).fieldsGrouping(wordExtractorId, new Fields("word"));
        builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(counterId, new Fields("obj"));
        builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
        
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
    	
    	XMLConfiguration config;
		try {
			if(args.length == 1)
				config = new XMLConfiguration(args[0]);
			else {
				ClassLoader classLoader = DICECrawler.class.getClassLoader();
				File configFile = new File(classLoader.getResource("dice.topic-detector.xml").getFile());
				config = new XMLConfiguration(configFile);
			}
		}
		catch(ConfigurationException ex) {
			ex.printStackTrace();
			return;
		}
		
    	Config topologyConfig = new Config();
    	topologyConfig.setDebug(false);
        
        StormTopology topology = createTopology(config);
        
    	LocalCluster cluster = new LocalCluster();
    	cluster.submitTopology("DiceTopicDetection", topologyConfig, topology);
    	Thread.sleep((long) DEFAULT_RUNTIME_IN_SECONDS * 1000);
    	cluster.killTopology("DiceTopicDetection");
    	cluster.shutdown();

    }
}