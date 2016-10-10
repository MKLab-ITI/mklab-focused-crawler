package gr.iti.mklab.focused.crawler.bolts.items;

import gr.iti.mklab.focused.crawler.bolts.structures.NthLastModifiedTimeTracker;
import gr.iti.mklab.focused.crawler.bolts.structures.SlidingWindowCounter;
import gr.iti.mklab.focused.crawler.utils.AnchorsTracker;
import gr.iti.mklab.focused.crawler.utils.TupleHelpers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * This bolt performs rolling counts of incoming objects, i.e. sliding window based counting.
 * 
 * The bolt is configured by two parameters, the length of the sliding window in seconds (which influences the output
 * data of the bolt, i.e. how it will count objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window length is set to an equivalent of five
 * minutes and the emit frequency to one minute, then the bolt will output the latest five-minute sliding window every
 * minute.
 * 
 * The bolt emits a rolling count tuple per object, consisting of the object itself, its latest rolling count, and the
 * actual duration of the sliding window. The latter is included in case the expected sliding window length (as
 * configured by the user) is different from the actual length, e.g. due to high system load. Note that the actual
 * window length is tracked and calculated for the window, and not individually for each object within a window.
 * 
 * Note: During the startup phase you will usually observe that the bolt warns you about the actual sliding window
 * length being smaller than the expected length. This behavior is expected and is caused by the way the sliding window
 * counts are initially "loaded up". You can safely ignore this warning during startup (e.g. you will see this warning
 * during the first ~ five minutes of startup time if the window length is set to five minutes).
 * 
 */
public class TermsRollingCountBolt extends BaseRichBolt {

    private static final long serialVersionUID = 5537727428628598519L;
    
    private static final Logger LOG = Logger.getLogger(TermsRollingCountBolt.class);
    
    private static final int NUM_WINDOW_CHUNKS = 5;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;

    private final SlidingWindowCounter<Object> counter;
    private final AnchorsTracker<Object> anchorsTracker; 
    
    private final int windowLengthInSeconds;
    private final int emitFrequencyInSeconds;
    private final int numWidnowChunks;
    
    private OutputCollector collector;
    
    private NthLastModifiedTimeTracker lastModifiedTracker;

    public TermsRollingCountBolt() {
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public TermsRollingCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        
        this.numWidnowChunks = deriveNumWindowChunksFrom(this.windowLengthInSeconds, this.emitFrequencyInSeconds);
        counter = new SlidingWindowCounter<Object>(numWidnowChunks);
        
        anchorsTracker = new AnchorsTracker<Object>();
    }

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
        return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds, this.emitFrequencyInSeconds));
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            LOG.info("Received tick tuple, triggering emit of current window counts");
            emitCurrentWindowCounts();
        }
        else {
            countObjAndAck(tuple);
        }
    }

    private void emitCurrentWindowCounts() {
    	
    	Map<Object, Long> currentCounts = counter.getCurrentCounts();
        Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format("Actual window length is %d seconds when it should be %d seconds (you can safely ignore this warning during the startup phase)", actualWindowLengthInSeconds, windowLengthInSeconds));
        }
        
        Map<Object, List<Tuple>> anchorsPerObject = anchorsTracker.getAnchorsThenReset();
        emit(counts, currentCounts, anchorsPerObject, actualWindowLengthInSeconds);
    }

    private void emit(Map<Object, Long> counts, Map<Object, Long> currentCounts, Map<Object, List<Tuple>> anchorsPerObject, int actualWindowLengthInSeconds) {
        for (Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            Long currentCount = currentCounts.get(obj);
            
            Long avgCount = Math.abs(count - currentCount) / (numWidnowChunks - 1);
            
            double chiSquare = Math.pow((currentCount - avgCount), 2) / (double) (avgCount + 1);
            
            List<Tuple> anchors = anchorsPerObject.get(obj);
            if(anchors != null) {
            	collector.emit(anchors, new Values(obj, count, chiSquare, actualWindowLengthInSeconds));
            }
            else {
            	collector.emit(new Values(obj, count, chiSquare, actualWindowLengthInSeconds));
            }
        }
    }

    private void countObjAndAck(Tuple tuple) {
        Object obj = tuple.getValue(0);
        counter.incrementCount(obj);
        anchorsTracker.addTuple(obj, tuple);
        
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count", "chiSquare", "actualWindowLengthInSeconds"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
    
}