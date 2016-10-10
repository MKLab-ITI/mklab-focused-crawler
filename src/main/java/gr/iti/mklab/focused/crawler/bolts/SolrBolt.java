package gr.iti.mklab.focused.crawler.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.storm.solr.config.CountBasedCommit;
import org.apache.storm.solr.config.SolrCommitStrategy;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SolrBolt extends BaseRichBolt {
    /**
	 * 
	 */
	private static final long serialVersionUID = 5902629460503610420L;

    /**
     * Half of the default Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS
     */
    private static final int DEFAULT_TICK_TUPLE_INTERVAL_SECS = 15;

    private final String service;
    
    private final SolrCommitStrategy commitStgy;    // if null, acks every tuple

    private SolrClient solrClient;
    private OutputCollector collector;
    private List<Tuple> toCommitTuples;

	private String collection;

	private Class<?> c;
	private String inputField;

    public SolrBolt(String service, String collection, Class<?> c, String inputField) {
        this(service, collection, null, c, inputField);
    }

    public SolrBolt(String service, String collection, SolrCommitStrategy commitStgy, Class<?> c, String inputField) {
        this.service = service;
        this.collection = collection;
        
        this.commitStgy = commitStgy;
        this.c = c;
        this.inputField = inputField;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.solrClient = new HttpSolrClient(service);
        this.toCommitTuples = new ArrayList<Tuple>(capacity());
    }

    private int capacity() {
        final int defArrListCpcty = 10;
        return (commitStgy instanceof CountBasedCommit) ?
                ((CountBasedCommit)commitStgy).getThreshold() :
                defArrListCpcty;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (!TupleUtils.isTick(tuple)) {    // Don't add tick tuples to the SolrRequest
            	
            	Object data = tuple.getValueByField(inputField);
            	Constructor<?> constructor = Class.forName(c.getName()).getConstructor(data.getClass());
				Object k = constructor.newInstance(data);
            	solrClient.addBean(collection, k);
            }
            ack(tuple);
        } catch (Exception e) {
            fail(tuple, e);
        }
    }

    private void ack(Tuple tuple) throws SolrServerException, IOException {
        if (commitStgy == null) {
            collector.ack(tuple);
        } else {
            final boolean isTickTuple = TupleUtils.isTick(tuple);
            if (!isTickTuple) {    // Don't ack tick tuples
                toCommitTuples.add(tuple);
                commitStgy.update();
            }
            
            if (isTickTuple || commitStgy.commit()) {
                solrClient.commit(collection);
                ackCommittedTuples();
            }
        }
    }

    private void ackCommittedTuples() {
        List<Tuple> toAckTuples = getQueuedTuples();
        for (Tuple tuple : toAckTuples) {
            collector.ack(tuple);
        }
    }

    private void fail(Tuple tuple, Exception e) {
        collector.reportError(e);

        if (commitStgy == null) {
            collector.fail(tuple);
        } else {
            List<Tuple> failedTuples = getQueuedTuples();
            failQueuedTuples(failedTuples);
        }
    }

    private void failQueuedTuples(List<Tuple> failedTuples) {
        for (Tuple failedTuple : failedTuples) {
            collector.fail(failedTuple);
        }
    }

    private List<Tuple> getQueuedTuples() {
        List<Tuple> queuedTuples = toCommitTuples;
        toCommitTuples = new ArrayList<Tuple>(capacity());
        return queuedTuples;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), DEFAULT_TICK_TUPLE_INTERVAL_SECS);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { 
    	
    }

}
