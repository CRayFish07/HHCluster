package edu.ub.ahstfg.io.index;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

import edu.ub.ahstfg.io.document.ParsedDocument;

public class NewIndex {
    
    private static final int ZERO = 0;
    private static final boolean TERM_COUNTER = false;
    private static final boolean KEYWORD_COUNTER = true;
    
    private HashMap<String, Integer> documents;
    private int docCounter;
    
    private HashMap<String, Integer> terms;
    private HashMap<Integer, HashMap<Integer, Short>> termFreq;
    private int termCounter;
    private String[] termVector;
    
    private HashMap<String, Integer> keywords;
    private HashMap<Integer, HashMap<Integer, Short>> keywordFreq;
    private int keywordCounter;
    private String[] keywordVector;
    
    public NewIndex() {
        documents = new HashMap<String, Integer>();
        docCounter = 0;
        
        terms = new HashMap<String, Integer>();
        termFreq = new HashMap<Integer, HashMap<Integer, Short>>();
        termCounter = 0;
        
        keywords = new HashMap<String, Integer>();
        keywordFreq = new HashMap<Integer, HashMap<Integer, Short>>();
        keywordCounter = 0;
    }
    
    public void addDocument(ParsedDocument doc) {
        String url = doc.getUrl().toString();
        if (!documents.containsKey(url)) {
            documents.put(url, docCounter);
            docCounter++;
        }
        int docID = documents.get(url);
        addItems(docID, doc.getTermMap(), terms, termFreq, TERM_COUNTER);
        addItems(docID, doc.getKeywordMap(), keywords, keywordFreq, KEYWORD_COUNTER);
    }
    
    private void addItems(int docID, HashMap<String, Short> docItemMap,
            HashMap<String, Integer> items,
            HashMap<Integer, HashMap<Integer, Short>> itemFreq,
            boolean itemCounter) {
        Set<String> docItems = docItemMap.keySet();
        int itemID;
        short freq;
        HashMap<Integer, Short> itemMap;
        for (String item : docItems) {
            freq = docItemMap.get(item);
            if (!items.containsKey(item)) {
                items.put(item, getItemCounter(itemCounter));
                itemFreq.put(getItemCounter(itemCounter), new HashMap<Integer, Short>());
                incrementItemCounter(itemCounter);
            }
            itemID = items.get(item);
            itemMap = itemFreq.get(itemID);
            itemMap.put(docID, freq);
        }
    }
    
    private int getItemCounter(boolean type) {
        if(type == TERM_COUNTER) {
            return termCounter;
        } else {
            return keywordCounter;
        }
    }
    
    private void incrementItemCounter(boolean type) {
        if(type == TERM_COUNTER) {
            termCounter++;
        } else {
            keywordCounter++;
        }
    }
    
    public void filter(double min, double max) throws IllegalArgumentException {
        if (min < 0 || min > 1 || max < 0 || max > 1 || min >= max) {
            throw new IllegalArgumentException("Min or max limits are incorrect.");
        }
        int termID, tDocs, nDocs = documents.size();
        double rate;
        LinkedList<String> toRemove = new LinkedList<String>();
        for (String term : terms.keySet()) {
            termID = terms.get(term);
            tDocs = termFreq.get(termID).size();
            rate = (double) tDocs / (double) nDocs;
            if (rate < min || rate > max) {
                toRemove.add(term);
            }
        }
        for(String termToRemove: toRemove) {
            termFreq.remove(terms.get(termToRemove));
            terms.remove(termToRemove);
        }
    }
    
    public FeatureDescriptor getFeatures() {
        termVector = new String[terms.size()];
        keywordVector = new String[keywords.size()];
        
        terms.keySet().toArray(termVector);
        keywords.keySet().toArray(keywordVector);
        
        return new FeatureDescriptor(termVector, keywordVector);
    }
    
    public DocumentDescriptor getFullDocument(String url) {
        if (!documents.containsKey(url)) {
            return null;
        }
        int docID = documents.get(url);
        short[] termVector = new short[terms.size()];
        short[] keywordVector = new short[keywords.size()];
        
        makeFreqVector(docID, termVector, terms, termFreq, this.termVector);
        makeFreqVector(docID, keywordVector, keywords, keywordFreq, this.keywordVector);
        
        return new DocumentDescriptor(url, termVector, keywordVector);
    }
    
    private static void makeFreqVector(int docID, short[] vector,
            HashMap<String, Integer> items,
            HashMap<Integer, HashMap<Integer, Short>> itemFreq,
            String[] itemVector) {
        int itemID;
        HashMap<Integer, Short> itemMap;
        String item;
        for(int i = 0; i < itemVector.length; i++) {
            item = itemVector[i];
            itemID = items.get(item);
            itemMap = itemFreq.get(itemID);
            if(itemMap.containsKey(docID)) {
                vector[i] = itemMap.get(docID);
            } else {
                vector[i] = ZERO;
            }
        }
    }
    
    public Set<String> getDocs() {
        return documents.keySet();
    }
    
    public int getNumDocs() {
        return documents.size();
    }
    
    public Set<String> getTerms() {
        return terms.keySet();
    }
    
    public int getNumTerms() {
        return terms.size();
    }
    
    public Set<String> getKeywords() {
        return keywords.keySet();
    }
    
    public int getNumKeywords() {
        return keywords.size();
    }
    
}
