package gr.iti.mklab.focused.crawler.spouts;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import static org.apache.storm.utils.Utils.tuple;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class MongoDbSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7261151120193254079L;
	
	private String _mongoHost;
	private String _mongoDbName;
	private String _mongoCollectionName;
	
	private SpoutOutputCollector _collector;
	
	private MongoClient _mongo = null;
	private MongoDatabase _database = null;

	private Bson _query;

	private LinkedBlockingQueue<Document> _queue;

	private CursorThread _listener = null;
	
	private MongoCollection<Document> _collection;
	
	public MongoDbSpout(String mongoHost, String mongoDbName, String mongoCollectionName, Bson query) {
		this._mongoHost = mongoHost;
		this._mongoDbName = mongoDbName;
		this._mongoCollectionName = mongoCollectionName;
		
		this._query = query;
	}

	
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		try {
			reset(_mongoHost, _mongoDbName, _mongoCollectionName);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		_collector = collector;
		_queue = new LinkedBlockingQueue<Document>(10000);
		
		_mongo = new MongoClient(_mongoHost);
		_database = _mongo.getDatabase(_mongoDbName);
		_collection = _database.getCollection(_mongoCollectionName);

		_listener  = new CursorThread(_queue, _database, _mongoCollectionName, _query);
		_listener.start();
	}

	public void nextTuple() {
		
		Document obj = _queue.poll();
		if(obj == null) {
            Utils.sleep(100);
        } else {    	
        	synchronized(_collector) {
        		_collector.emit(tuple(obj.toString()));
        	}
        	
    		_collection.updateOne(
    			new BasicDBObject("_id", obj.get("_id")),
    			new BasicDBObject("$set", new BasicDBObject("status", "injected"))
    		);
        }
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(_mongoCollectionName));	
	}
	
	@Override
    public void ack(Object url) {

    }

    @Override
    public void fail(Object url) {

    }
    
	class CursorThread extends Thread {

		LinkedBlockingQueue<Document> queue;
		String mongoCollectionName;
		MongoDatabase mongoDB;
		Bson query;
		
		public CursorThread(LinkedBlockingQueue<Document> queue, MongoDatabase mongoDB, String mongoCollectionName, Bson query) {
			
			this.queue = queue;
			this.mongoDB = mongoDB;
			this.mongoCollectionName = mongoCollectionName;
			this.query = query;
		}

		public void run() {
			while(true) {
				FindIterable<Document> cursor = mongoDB.getCollection(mongoCollectionName).find(query)
						.sort(new BasicDBObject("_id", -1)).limit(100);
				
				MongoCursor<Document> it = cursor.iterator();
				while(it.hasNext()) {			
					Document obj = it.next();
					if(obj != null) {
						try {
							queue.put(obj);
						} catch (InterruptedException e) {
							Utils.sleep(100);
						}
					}
				}
				System.out.println("Injector: " + _queue.size());
				Utils.sleep(10000);
			}
		};
	}
	
	public void reset(String host, String dbName, String collectionName) throws UnknownHostException {
		
		MongoClient client = new MongoClient(host);
		MongoDatabase db = client.getDatabase(dbName);
		MongoCollection<Document> collection = db.getCollection(collectionName);
		
		DBObject q = new BasicDBObject("status", "injected");
		DBObject o = new BasicDBObject("$set", new BasicDBObject("status", "new"));
		//collection.update(q, o, false, true);
		
	}
	
	
}
