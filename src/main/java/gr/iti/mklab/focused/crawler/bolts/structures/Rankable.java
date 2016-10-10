package gr.iti.mklab.focused.crawler.bolts.structures;

import org.bson.Document;

public interface Rankable extends Comparable<Rankable> {

	public Object getObject();

    public long getValue();
    
    public Document toDocument();

}