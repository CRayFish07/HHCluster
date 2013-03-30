package edu.ub.ahstfg.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Index implements Writable {

    private ArrayList<String> terms;
    private HashMap<String, ArrayList<Long>> termFreq;

    public Index() {
        terms = new ArrayList<String>();
        termFreq = new HashMap<String, ArrayList<Long>>();
    }

    public void addTerm(final String term, final String url, final long freq) {
        if (!termFreq.containsKey(url)) {
            ArrayList<Long> l = new ArrayList<Long>();
            for (int i = 0; i < terms.size(); i++) {
                l.add((long) 0);
            }
            termFreq.put(url, l);
        }
        if (terms.contains(term)) {
            final int index = terms.indexOf(term);
            ArrayList<Long> freqs = termFreq.get(url);
            freqs.set(index, freqs.get(index) + freq);
        } else {
            terms.add(term);
            final int index = terms.indexOf(term);
            ArrayList<Long> freqs;
            for (String storedUrl : termFreq.keySet()) {
                freqs = termFreq.get(storedUrl);
                if (storedUrl.equals(url)) {
                    freqs.add((long) 0);
                    freqs.set(index, freq);
                } else {
                    freqs.add((long) 0);
                }
            }
        }
    }

    public String[] getTermVector() {
        return terms.toArray(new String[terms.size()]);
    }

    public String[] getDocumentVector() {
        String[] ret = new String[termFreq.size()];
        int i = 0;
        for (String url : termFreq.keySet()) {
            ret[i] = url;
            i++;
        }
        return ret;
    }

    public long[][] getFreqMatrix() {
        long[][] ret = new long[termFreq.size()][terms.size()];
        int i = 0, j = 0;
        ArrayList<Long> freqs;
        for (String url : termFreq.keySet()) {
            freqs = termFreq.get(url);
            for (int k = 0; k < terms.size(); k++) {
                ret[i][j] = freqs.get(j);
                j++;
            }
            i++;
            j = 0;
        }
        return ret;
    }

    public static ArrayList<String> arrayWritable2ArrayListString(
            ArrayWritable input) {
        ArrayList<String> ret = new ArrayList<String>();
        Writable[] ws = input.get();
        Text t;
        for (Writable w : ws) {
            t = (Text) w;
            ret.add(t.toString());
        }
        return ret;
    }

    public static ArrayList<Long> arrayWritable2ArrayListLong(
            ArrayWritable input) {
        ArrayList<Long> ret = new ArrayList<Long>();
        Writable[] ws = input.get();
        LongWritable l;
        for (Writable w : ws) {
            l = (LongWritable) w;
            ret.add(l.get());
        }
        return ret;
    }

    public static HashMap<String, ArrayList<Long>> mapWritable2HashMapStringArrayListLong(
            MapWritable input) {
        HashMap<String, ArrayList<Long>> ret = new HashMap<String, ArrayList<Long>>();
        Text t;
        ArrayWritable aw;
        ArrayList<Long> al;
        for (Writable w : input.keySet()) {
            t = (Text) w;
            aw = (ArrayWritable) input.get(t);
            al = arrayWritable2ArrayListLong(aw);
            ret.put(t.toString(), al);
        }
        return ret;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        ArrayWritable wTerms = new ArrayWritable(Text.class);
        wTerms.readFields(input);
        terms = arrayWritable2ArrayListString(wTerms);

        MapWritable wTermFreq = new MapWritable();
        wTermFreq.readFields(input);
        termFreq = mapWritable2HashMapStringArrayListLong(wTermFreq);
    }

    public static ArrayWritable arrayListString2ArrayWritable(
            ArrayList<String> input) {
        ArrayWritable ret = new ArrayWritable(Text.class);
        Writable[] ws = new Writable[input.size()];
        for (int i = 0; i < input.size(); i++) {
            ws[i] = new Text(input.get(i));
        }
        ret.set(ws);
        return ret;
    }

    public static ArrayWritable arrayListLong2ArrayWritable(
            ArrayList<Long> input) {
        ArrayWritable ret = new ArrayWritable(LongWritable.class);
        Writable[] ws = new Writable[input.size()];
        for (int i = 0; i < input.size(); i++) {
            ws[i] = new LongWritable(input.get(i));
        }
        ret.set(ws);
        return ret;
    }

    public static MapWritable hashMapStringArrayListLong2MapWritable(
            HashMap<String, ArrayList<Long>> input) {
        MapWritable ret = new MapWritable();
        ArrayList<Long> arl;
        for (String s : input.keySet()) {
            arl = input.get(s);
            ret.put(new Text(s), arrayListLong2ArrayWritable(arl));
        }
        return ret;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        arrayListString2ArrayWritable(terms).write(output);
        hashMapStringArrayListLong2MapWritable(termFreq).write(output);
    }

}
