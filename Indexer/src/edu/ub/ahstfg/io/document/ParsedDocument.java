package edu.ub.ahstfg.io.document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Document representation passed between map and reduce indexing phases.
 * @author Alberto Huelamo Segura
 */
public class ParsedDocument implements Writable {
    
    private static final IntWritable ONE() {
        return new IntWritable(1);
    }
    
    private Text url;
    private MapWritable terms;
    private MapWritable keywords;
    
    /**
     * Unparametrized constructor.
     */
    public ParsedDocument() {
        url = new Text();
        terms = new MapWritable();
        keywords = new MapWritable();
    }
    
    /**
     * Instances new document specifying its URL.
     * @param url Document URL.
     */
    public ParsedDocument(Text url) {
        this();
        this.url = url;
    }
    
    /**
     * Instances new document specifying its URL.
     * @param url Document URL.
     */
    public ParsedDocument(String url) {
        this(new Text(url));
    }
    
    /**
     * Adds a new term. If the term exists increases their frequency.
     * @param term Term to add.
     */
    public void addTerm(Text term) {
        if (terms.containsKey(term)) {
            IntWritable n = (IntWritable) terms.get(term);
            n.set(n.get() + 1);
        } else {
            terms.put(term, ONE());
        }
    }
    
    /**
     * Adds a new term. If the term exists increases their frequency.
     * @param term Term to add.
     */
    public void addTerm(String term) {
        addTerm(new Text(term));
    }
    
    /**
     * Gets document url.
     * @return The url of the document.
     */
    public Text getUrl() {
        return url;
    }
    
    /**
     * Gets the HashMap containing term frequency.
     * @return Term frequency HashMap.
     */
    public HashMap<String, Short> getTermMap() {
        HashMap<String, Short> ret = new HashMap<String, Short>();
        Text t;
        IntWritable value;
        for (Writable w : terms.keySet()) {
            t = (Text) w;
            value = (IntWritable) terms.get(w);
            ret.put(t.toString(), (short)value.get());
        }
        return ret;
    }
    
    /**
     * Builds and gets a vector with added terms.
     * @return The term vector.
     */
    public String[] getTermVector() {
        String[] ret = new String[terms.size()];
        Text t;
        int i = 0;
        for (Writable w : terms.keySet()) {
            t = (Text) w;
            ret[i] = t.toString();
            i++;
        }
        return ret;
    }
    
    /**
     * Builds and gets a vector with added keywords.
     * @return The keyword vector.
     */
    public long[] getTermFreqVector() {
        long[] ret = new long[terms.size()];
        Text t;
        IntWritable v;
        int i = 0;
        for (Writable w : terms.keySet()) {
            t = (Text) w;
            v = (IntWritable) terms.get(t);
            ret[i] = v.get();
            i++;
        }
        return ret;
    }
    
    /**
     * Adds a new keyword. If the keyword exists increases their frequency.
     * @param term Keyword to add.
     */
    public void addKeyword(Text keyword) {
        if (keywords.containsKey(keyword)) {
            IntWritable n = (IntWritable) keywords.get(keyword);
            n.set(n.get() + 1);
        } else {
            keywords.put(keyword, ONE());
        }
    }
    
    /**
     * Adds a new keyword. If the keyword exists increases their frequency.
     * @param term Keyword to add.
     */
    public void addKeyword(String keyword) {
        addKeyword(new Text(keyword));
    }
    
    /**
     * Gets the HashMap containing keyword frequency.
     * @return Keyword frequency HashMap.
     */
    public HashMap<String, Short> getKeywordMap() {
        HashMap<String, Short> ret = new HashMap<String, Short>();
        Text t;
        IntWritable value;
        for (Writable w : keywords.keySet()) {
            t = (Text) w;
            value = (IntWritable) keywords.get(w);
            ret.put(t.toString(), (short)value.get());
        }
        return ret;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        url.readFields(input);
        terms.readFields(input);
        keywords.readFields(input);
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        url.write(output);
        terms.write(output);
        keywords.write(output);
    }
    
}
