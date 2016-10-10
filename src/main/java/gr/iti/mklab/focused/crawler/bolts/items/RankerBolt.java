package gr.iti.mklab.focused.crawler.bolts.items;

import gr.iti.mklab.focused.crawler.bolts.structures.Rankings;
import gr.iti.mklab.focused.crawler.utils.TupleHelpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * This abstract bolt provides the basic behavior of bolts that rank objects according to their count.
 * 
 * It uses a template method design pattern for {@link RankerBolt#execute(Tuple, BasicOutputCollector)} to allow
 * actual bolt implementations to specify how incoming tuples are processed, i.e. how the objects embedded within those
 * tuples are retrieved and counted.
 * 
 */
public abstract class RankerBolt extends BaseRichBolt {

    private static final long serialVersionUID = 4931640198501530202L;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int DEFAULT_COUNT = 10;

    private final int emitFrequencyInSeconds;
    private final int count;
    
    private final Rankings rankings;

    private List<Tuple> anchors = new ArrayList<Tuple>();
	private OutputCollector collector;
    
	private boolean change = false;
	
    public RankerBolt() {
        this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public RankerBolt(int topN) {
        this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public RankerBolt(int topN, int emitFrequencyInSeconds) {
        if (topN < 1) {
            throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
        }
        if (emitFrequencyInSeconds < 1) {
            throw new IllegalArgumentException("The emit frequency must be >= 1 seconds (you requested "
                + emitFrequencyInSeconds + " seconds)");
        }
        
        this.count = topN;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        this.rankings = new Rankings(count);
        
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	this.collector = collector;
    }
    
    protected Rankings getRankings() {
        return rankings;
    }

    /**
     * This method functions as a template method (design pattern).
     */
    @Override
    public final void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            getLogger().info("Received tick tuple, triggering emit of current rankings");
            if(change) {
            	emitRankings();
            	change = false;
            } else {
            	getLogger().info("Nothing has been changed. Cannot emit yet.");
            }
        }
        else {
        	anchors.add(tuple);
            updateRankingsWithTuple(tuple);
            change = true;
        }
    }

    abstract void updateRankingsWithTuple(Tuple tuple);

    private void emitRankings() {
        collector.emit(anchors, new Values(rankings));
        anchors.clear();
        
        getLogger().info("Rankings: " + rankings);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rankings"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }

    abstract Logger getLogger();
}