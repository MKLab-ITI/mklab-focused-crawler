package gr.iti.mklab.focused.crawler.bolts.tools;

public interface Rankable extends Comparable<Rankable> {

    Object getObject();

    long getCount();

}