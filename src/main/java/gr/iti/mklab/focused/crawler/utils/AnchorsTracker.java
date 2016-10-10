package gr.iti.mklab.focused.crawler.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.tuple.Tuple;

public class AnchorsTracker<T> implements Serializable {
    
    /**
	 * 
	 */
	private static final long serialVersionUID = 4592522442976516779L;
	private final Map<Object, List<Tuple>> anchorsPerObject = new HashMap<Object, List<Tuple>>();
    
	public void addTuple(T obj, Tuple tuple) {
        List<Tuple> anchors = anchorsPerObject.get(obj);
        if(anchors == null) {
        	anchors = new ArrayList<Tuple>();
        	anchorsPerObject.put(obj, anchors);
        }
        anchors.add(tuple);
    }
	
    public Map<Object, List<Tuple>> getAnchorsThenReset() {
    	Map<Object, List<Tuple>> apObj = new HashMap<Object, List<Tuple>>();
    	for(Entry<Object, List<Tuple>> entry : anchorsPerObject.entrySet()) {
    		apObj.put(entry.getKey(), entry.getValue());
    	}

    	anchorsPerObject.clear();
    	
        return apObj;
    }
}

