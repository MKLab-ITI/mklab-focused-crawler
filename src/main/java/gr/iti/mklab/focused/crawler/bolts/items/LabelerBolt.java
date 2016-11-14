package gr.iti.mklab.focused.crawler.bolts.items;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class LabelerBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3140031284923861710L;

	private String _mongoHost;
	private String _mongoDbName;
	private String _mongoCollectionName;
	
	private String _solrService;
	private String _solrCollection;
	
	private MongoClient _mongo;
	private MongoDatabase _database;
	private MongoCollection<Document> _collection;
	private HttpSolrClient _solrClient;
	
	private OutputCollector _collector;

	private Logger _logger;

	private String _field;
	
	public LabelerBolt(String mongoHost, String mongoDbName, String mongoCollectionName,
			String solrService, String solrCollection, String field) {
		
		_mongoHost = mongoHost;
		_mongoDbName = mongoDbName;
		_mongoCollectionName = mongoCollectionName;
		
		_solrService = solrService;
		_solrCollection = solrCollection;
		_field = field;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		
		_collector = collector; 
		
		this._logger = Logger.getLogger(LabelerBolt.class);
		
		_mongo = new MongoClient(_mongoHost);
		_database = _mongo.getDatabase(_mongoDbName);
		_collection = _database.getCollection(_mongoCollectionName);
		
        _solrClient = new HttpSolrClient(_solrService);
	}

	@Override
	public void execute(Tuple input) {
		_collector.ack(input);

		Document document = (Document) input.getValueByField("documents");
		String obj = document.getString("obj");
		Long timestamp = document.getLong("timestamp");
		
		String label = getlabel(obj);
		if(label != null) {
			Document filter = new Document("obj", obj);
			filter.append("timestamp", timestamp);
			Document update = new Document("$set", new Document("label", label));
			_collection.updateOne(filter, update);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		
	}

	private String getlabel(String q) {
		try {
			SolrQuery query = new SolrQuery(_field + ":(" + q + ")");
			query.addSort("shares", ORDER.desc);
			
			QueryResponse resp = _solrClient.query(_solrCollection, query);
			SolrDocumentList results = resp.getResults();
			for(SolrDocument doc : results) {
				String text = (String) doc.get("title");
				return text;
			}
			
		} catch (SolrServerException | IOException e) {
			_logger.error(e);
		}
		return null;
	}
}
