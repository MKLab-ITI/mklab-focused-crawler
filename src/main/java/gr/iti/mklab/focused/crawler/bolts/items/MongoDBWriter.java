package gr.iti.mklab.focused.crawler.bolts.items;

import gr.iti.mklab.focused.crawler.bolts.structures.Rankable;
import gr.iti.mklab.focused.crawler.bolts.structures.Rankings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDBWriter extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6276448375491257043L;
	
	private String _mongoHost;
	private String _mongoDbName;
	private String _mongoCollectionName;
	
	private MongoClient _mongo;
	private MongoDatabase _database;
	private MongoCollection<Document> _collection;

	private OutputCollector _collector;

	public MongoDBWriter(String mongoHost, String mongoDbName, String mongoCollectionName) {
		_mongoHost = mongoHost;
		_mongoDbName = mongoDbName;
		_mongoCollectionName = mongoCollectionName;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		
		_mongo = new MongoClient(_mongoHost);
		_database = _mongo.getDatabase(_mongoDbName);
		_collection = _database.getCollection(_mongoCollectionName);
		
	}

	@Override
	public void execute(Tuple input) {
		
		_collector.ack(input);
		
		Rankings rankings = (Rankings) input.getValue(0);
		
		List<Document> documents = new ArrayList<Document>();
		long timestamp = System.currentTimeMillis();
		for(Rankable r : rankings.getRankings()) {
			
			Document document = r.toDocument();
			document.append("timestamp", timestamp);

			documents.add(document);
			
			_collector.emit(input, new Values(document));
		} 
		_collection.insertMany(documents);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("documents"));
	}

}
