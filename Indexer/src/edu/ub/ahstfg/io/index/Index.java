package edu.ub.ahstfg.io.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.ub.ahstfg.io.WritableConverter;

public class Index implements Writable {

    private ArrayList<String> terms;
    private HashMap<String, ArrayList<Long>> termFreq;

    private HashMap<String, ArrayList<String>> termAppearance;

    private ArrayList<String> keywords;
    private HashMap<String, ArrayList<Long>> keywordFreq;

    public Index() {
        terms = new ArrayList<String>();
        termFreq = new HashMap<String, ArrayList<Long>>();

        termAppearance = new HashMap<String, ArrayList<String>>();

        keywords = new ArrayList<String>();
        keywordFreq = new HashMap<String, ArrayList<Long>>();
    }

    public void addTerm(final String term, final String url, final long freq) {
        addTermAppearance(term, url);
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

    private void addTermAppearance(final String url, final String term) {
        if (!termAppearance.containsKey(term)) {
            termAppearance.put(term, new ArrayList<String>());
        }
        ArrayList<String> docs = termAppearance.get(term);
        if (!docs.contains(url)) {
            docs.add(url);
        }
    }

    public String[] getTermVector() {
        return terms.toArray(new String[terms.size()]);
    }

    public String[] getDocumentTermVector() {
        String[] ret = new String[termFreq.size()];
        int i = 0;
        for (String url : termFreq.keySet()) {
            ret[i] = url;
            i++;
        }
        return ret;
    }

    public long[][] getTermFreqMatrix() {
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

    public void removeTerm(String term) {
        int idx = terms.indexOf(term);
        terms.remove(idx);
        ArrayList<Long> tf;
        for (String url : termFreq.keySet()) {
            tf = termFreq.get(url);
            tf.remove(idx);
        }
    }

    public void filter(double infCote, double supCote) {
        int nDocs, totalDocs = termFreq.size();
        double rate;
        for (String term : termAppearance.keySet()) {
            nDocs = termAppearance.get(term).size();
            rate = (double) nDocs / (double) totalDocs;
            if (rate < infCote || rate > supCote) {
                removeTerm(term);
            }
        }
    }

    public void addKeyword(final String keyword, final String url,
            final long freq) {
        if (!keywordFreq.containsKey(url)) {
            ArrayList<Long> l = new ArrayList<Long>();
            for (int i = 0; i < terms.size(); i++) {
                l.add((long) 0);
            }
            keywordFreq.put(url, l);
        }
        if (keywords.contains(keyword)) {
            final int index = keywords.indexOf(keyword);
            ArrayList<Long> freqs = keywordFreq.get(url);
            freqs.set(index, freqs.get(index) + freq);
        } else {
            keywords.add(keyword);
            final int index = keywords.indexOf(keyword);
            ArrayList<Long> freqs;
            for (String storedUrl : keywordFreq.keySet()) {
                freqs = keywordFreq.get(storedUrl);
                if (storedUrl.equals(url)) {
                    freqs.add((long) 0);
                    freqs.set(index, freq);
                } else {
                    freqs.add((long) 0);
                }
            }
        }
    }

    public String[] getKeywordVector() {
        return keywords.toArray(new String[keywords.size()]);
    }

    public long[][] getKeywordFreqMatrix() {
        long[][] ret = new long[termFreq.size()][terms.size()];
        int i = 0, j = 0;
        ArrayList<Long> freqs;
        for (String url : termFreq.keySet()) {
            freqs = keywordFreq.get(url);
            if (freqs != null) {
                for (int k = 0; k < keywords.size(); k++) {
                    ret[i][j] = freqs.get(j);
                    j++;
                }
            } else {
                ret[i] = new long[] { -1 };
            }
            i++;
            j = 0;
        }
        return ret;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        ArrayWritable wTerms = new ArrayWritable(Text.class);
        wTerms.readFields(input);
        terms = WritableConverter.arrayWritable2ArrayListString(wTerms);

        MapWritable wTermFreq = new MapWritable();
        wTermFreq.readFields(input);
        termFreq = WritableConverter
                .mapWritable2HashMapStringArrayListLong(wTermFreq);

        ArrayWritable wKeywords = new ArrayWritable(Text.class);
        wKeywords.readFields(input);
        keywords = WritableConverter.arrayWritable2ArrayListString(wKeywords);

        MapWritable wKeywordFreq = new MapWritable();
        wKeywordFreq.readFields(input);
        keywordFreq = WritableConverter
                .mapWritable2HashMapStringArrayListLong(wKeywordFreq);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        WritableConverter.arrayListString2ArrayWritable(terms).write(output);
        WritableConverter.hashMapStringArrayListLong2MapWritable(termFreq)
                .write(output);
        WritableConverter.arrayListString2ArrayWritable(keywords).write(output);
        WritableConverter.hashMapStringArrayListLong2MapWritable(keywordFreq)
                .write(output);
    }

}
