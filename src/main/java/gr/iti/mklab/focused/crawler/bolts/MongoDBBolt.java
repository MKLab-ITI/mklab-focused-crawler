package gr.iti.mklab.focused.crawler.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import gr.iti.mklab.framework.common.domain.JSONable;

public class MongoDBBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9028014271207845500L;
	
	private static final int DEFAULT_TICK_TUPLE_INTERVAL_SECS = 10;
	
	private OutputCollector _collector;
	
	private String _mongoHost;
	private String _mongoDbName;
	private String _mongoCollectionName;
	private MongoClient _mongo;
	private MongoDatabase _database;
	private MongoCollection<DBObject> _collection;
	
	private List<Tuple> tuplesToBeSaved = new ArrayList<Tuple>();

	public MongoDBBolt(String mongoHost, String mongoDbName, String mongoCollectionName) {
		this._mongoHost = mongoHost;
		this._mongoDbName = mongoDbName;
		this._mongoCollectionName = mongoCollectionName;	
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		
		_mongo = new MongoClient(_mongoHost);
		_database = _mongo.getDatabase(_mongoDbName);
		_collection = _database.getCollection(_mongoCollectionName, DBObject.class);
	}

	@Override
	public void execute(Tuple input) {
		if (!TupleUtils.isTick(input)) {
			tuplesToBeSaved.add(input);
		}
		else {
			save();
		}
	}

	private void save() {
		List<DBObject> docs = new ArrayList<DBObject>();
		for(Tuple tuple : tuplesToBeSaved) {
			JSONable json = (JSONable) tuple.getValue(0);
			docs.add(json.toDBObject());
			
			_collector.ack(tuple);
		}
		if(!docs.isEmpty()) {
			try {
				_collection.insertMany(docs);	
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
    }
	
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), DEFAULT_TICK_TUPLE_INTERVAL_SECS);
    }
    
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
