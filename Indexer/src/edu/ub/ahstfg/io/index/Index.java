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
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.WritableConverter;

/**
 * Index class. Groups all terms and keywords and constructs the feature matrix.
 * @author Alberto Huelamo Segura
 */
public class Index implements Writable {
    
    private ArrayList<String> terms;
    private HashMap<String, ArrayList<Long>> termFreq;
    
    private HashMap<String, ArrayList<String>> termAppearance;
    
    private ArrayList<String> keywords;
    private HashMap<String, ArrayList<Long>> keywordFreq;
    
    private Reporter reporter;
    
    /**
     * Sole constructor.
     * @param reporter Hadoop job reporter.
     */
    public Index(Reporter reporter) {
        terms = new ArrayList<String>();
        termFreq = new HashMap<String, ArrayList<Long>>();
        
        termAppearance = new HashMap<String, ArrayList<String>>();
        
        keywords = new ArrayList<String>();
        keywordFreq = new HashMap<String, ArrayList<Long>>();
        
        this.reporter = reporter;
        reporter.incrCounter("Index report", "Remove term request", 0);
        reporter.incrCounter("Index report", "Removed terms", 0);
    }
    
    /**
     * Adds a term from a document to the index.
     * If the term already exists increases their frequency.
     * @param term Term to add.
     * @param url Document where term appears.
     * @param freq Frequency to increase.
     */
    public void addTerm(final String term, final String url, final long freq) {
        addTermAppearance(url, term);
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
    
    /**
     * Adds a document to term appearace. Used to filter.
     * @param url Docuemnt URL to add.
     * @param term Term which appears in document.
     */
    private void addTermAppearance(final String url, final String term) {
        if (!termAppearance.containsKey(term)) {
            termAppearance.put(term, new ArrayList<String>());
        }
        ArrayList<String> docs = termAppearance.get(term);
        if (!docs.contains(url)) {
            docs.add(url);
        }
    }
    
    /**
     * Gets the term vector.
     * @return An array of terms.
     */
    public String[] getTermVector() {
        return terms.toArray(new String[terms.size()]);
    }
    
    /**
     * Gets the document urls which have terms.
     * @return An array of urls.
     */
    public String[] getDocumentTermVector() {
        String[] ret = new String[termFreq.size()];
        int i = 0;
        for (String url : termFreq.keySet()) {
            ret[i] = url;
            i++;
        }
        return ret;
    }
    
    /**
     * Gets the term frequency matrix.
     * @return An array of arrays with frequencies. Rows represents the documents.
     */
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
    
    /**
     * Removes a term from all documents.
     * @param term The term to remove.
     */
    public void removeTerm(String term) {
        int idx = terms.indexOf(term);
        if(idx > 0) {
            terms.remove(idx);
            ArrayList<Long> tf;
            for (String url : termFreq.keySet()) {
                tf = termFreq.get(url);
                tf.remove(idx);
            }
            reporter.incrCounter("Index report", "Removed terms", 1);
        }
    }
    
    /**
     * Filters the index. Removes those terms which appears in less documents
     * than a percentage and those which appears in more documents than other
     * percentage.
     * @param infCote Terms which appear in less documents than this percentage will be removed.
     * @param supCote Terms which appear in more documents than this percentage will be removed.
     */
    public void filter(double infCote, double supCote) {
        int nDocs, totalDocs = termFreq.size();
        double rate;
        for (String term : termAppearance.keySet()) {
            nDocs = termAppearance.get(term).size();
            rate = (double) nDocs / (double) totalDocs;
            if (rate < infCote || rate > supCote) {
                removeTerm(term);
                reporter.incrCounter("Index report", "Remove term request", 1);
            }
        }
    }
    
    /**
     * Adds a keyword from a document to the index.
     * If the keyword already exists increases their frequency.
     * @param keyword Keyword to add.
     * @param url Document where keyword appears.
     * @param freq Frequency to increase.
     */
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
    
    /**
     * Gets the vector of the added keywords.
     * @return An array with the keywords.
     */
    public String[] getKeywordVector() {
        return keywords.toArray(new String[keywords.size()]);
    }
    
    /**
     * Gets the keyword frequency matrix.
     * @return An array of arrays with frequencies. Rows represents the documents.
     */
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
