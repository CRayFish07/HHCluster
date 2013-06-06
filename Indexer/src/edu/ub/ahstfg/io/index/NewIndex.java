package edu.ub.ahstfg.io.index;

import java.util.HashMap;
import java.util.Set;

import edu.ub.ahstfg.io.document.ParsedDocument;

public class NewIndex {
    
    private static final int ZERO = 0;
    
    private HashMap<String, Integer> documents;
    private int docCounter;
    
    private HashMap<String, Integer> terms;
    private HashMap<Integer, HashMap<Integer, Short>> termFreq;
    private int termCounter;
    
    private HashMap<String, Integer> keywords;
    private HashMap<Integer, HashMap<Integer, Short>> keywordFreq;
    private int keywordCounter;
    
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
        addItems(docID, doc.getTermMap(), terms, termFreq, termCounter);
        addItems(docID, doc.getKeywordMap(), keywords, keywordFreq, keywordCounter);
    }
    
    private void addItems(int docID, HashMap<String, Short> docItemMap,
            HashMap<String, Integer> items,
            HashMap<Integer, HashMap<Integer, Short>> itemFreq, int itemCounter) {
        Set<String> docItems = docItemMap.keySet();
        int itemID;
        short freq;
        HashMap<Integer, Short> itemMap;
        for (String item : docItems) {
            freq = docItemMap.get(item);
            if (!items.containsKey(item)) {
                items.put(item, itemCounter);
                itemFreq.put(itemCounter, new HashMap<Integer, Short>());
                itemCounter++;
            }
            itemID = items.get(item);
            itemMap = itemFreq.get(itemID);
            itemMap.put(docID, freq);
        }
    }
    
    public void filter(double min, double max) throws IllegalArgumentException {
        if (min < 0 || min > 1 || max < 0 || max > 1 || min >= max) {
            throw new IllegalArgumentException("Min or max limits are incorrect.");
        }
        int termID, tDocs, nDocs = documents.size();
        double rate;
        for (String term : terms.keySet()) {
            termID = terms.get(term);
            tDocs = termFreq.get(termID).size();
            rate = (double) tDocs / (double) nDocs;
            if (rate < min || rate > max) {
                terms.remove(term);
                termFreq.remove(termID);
            }
        }
    }
    
    public DocumentDescriptor getFullDocument(String url) {
        if (!documents.containsKey(url)) {
            return null;
        }
        int docID = documents.get(url);
        short[] termVector = new short[terms.size()];
        short[] keywordVector = new short[keywords.size()];
        
        makeVector(docID, termVector, terms, termFreq);
        makeVector(docID, keywordVector, keywords, keywordFreq);
        
        return new DocumentDescriptor(url, termVector, keywordVector);
    }
    
    public void makeVector(int docID, short[] vector,
            HashMap<String, Integer> items,
            HashMap<Integer, HashMap<Integer, Short>> itemFreq) {
        int itemID;
        HashMap<Integer, Short> itemMap;
        for(String item: items.keySet()) {
            itemID = items.get(item);
            itemMap = itemFreq.get(itemID);
            if(itemMap.containsKey(docID)) {
                vector[itemID] = itemMap.get(docID);
            } else {
                vector[itemID] = ZERO;
            }
        }
    }
    
}
