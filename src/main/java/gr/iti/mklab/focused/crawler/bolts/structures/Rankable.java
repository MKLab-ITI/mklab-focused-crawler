package gr.iti.mklab.focused.crawler.bolts.structures;

public interface Rankable extends Comparable<Rankable> {

    Object getObject();

    long getCount();

}