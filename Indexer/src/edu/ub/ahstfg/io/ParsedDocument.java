package edu.ub.ahstfg.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ParsedDocument implements Writable {

    private static final LongWritable ONE() {
        return new LongWritable(1);
    }

    private MapWritable terms;
    private MapWritable keywords;

    public ParsedDocument() {
        terms = new MapWritable();
        keywords = new MapWritable();
    }

    public void addTerm(Text term) {
        if (terms.containsKey(term)) {
            LongWritable n = (LongWritable) terms.get(term);
            terms.put(term, new LongWritable(n.get() + 1));
        } else {
            terms.put(term, ONE());
        }
    }

    public void addTerm(String term) {
        addTerm(new Text(term));
    }

    public HashMap<String, Long> getTermMap() {
        HashMap<String, Long> ret = new HashMap<String, Long>();
        Text t;
        LongWritable value;
        for (Writable w : terms.keySet()) {
            t = (Text) w;
            value = (LongWritable) terms.get(w);
            ret.put(t.toString(), value.get());
        }
        return ret;
    }

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

    public long[] getTermFreqVector() {
        long[] ret = new long[terms.size()];
        Text t;
        LongWritable v;
        int i = 0;
        for (Writable w : terms.keySet()) {
            t = (Text) w;
            v = (LongWritable) terms.get(t);
            ret[i] = v.get();
            i++;
        }
        return ret;
    }

    public void addKeyword(Text keyword) {
        if (keywords.containsKey(keyword)) {
            LongWritable n = (LongWritable) keywords.get(keyword);
            keywords.put(keyword, new LongWritable(n.get() + 1));
        } else {
            keywords.put(keyword, ONE());
        }
    }

    public void addKeyword(String keyword) {
        addKeyword(new Text(keyword));
    }

    public HashMap<String, Long> getKeywordMap() {
        HashMap<String, Long> ret = new HashMap<String, Long>();
        Text t;
        LongWritable value;
        for (Writable w : keywords.keySet()) {
            t = (Text) w;
            value = (LongWritable) keywords.get(w);
            ret.put(t.toString(), value.get());
        }
        return ret;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        terms.readFields(input);
        keywords.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        terms.write(output);
        keywords.write(output);
    }

}
