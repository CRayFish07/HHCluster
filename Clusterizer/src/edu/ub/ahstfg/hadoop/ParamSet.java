package edu.ub.ahstfg.hadoop;

import java.util.HashMap;

import org.apache.hadoop.mapred.JobConf;

public class ParamSet {
    
    // Param keys
    public static final String OLD_CENTROIDS_PATH  = "old_centroids_path";
    public static final String NEW_CENTROIDS_PATH  = "new_centroids_path";
    public static final String K                   = "K";
    public static final String NUM_DOCS            = "nDocs";
    public static final String NUM_KEYWORDS        = "nKeywords";
    public static final String NUM_TERMS           = "nTerms";
    public static final String NUM_MACHINES        = "nMachines";
    public static final String WEIGHT_KEYWORDS     = "wKeywords";
    public static final String WEIGHT_TERMS        = "wTerms";
    public static final String N_ITERATION         = "nIter";
    
    // Param default values
    public static final String INPUT_PATH          = "kmeans_input";
    public static final String OUTPUT_PATH         = "kmeans_output";
    
    private HashMap<String, String>  strings;
    private HashMap<String, Integer> ints;
    private HashMap<String, Float>   floats;
    
    public ParamSet() {
        strings = new HashMap<String, String>();
        ints    = new HashMap<String, Integer>();
        floats  = new HashMap<String, Float>();
    }
    
    public String getString(String key) {
        return strings.get(key);
    }
    
    public void setString(String key, String value) {
        strings.put(key, value);
    }
    
    public int getInt(String key) {
        return ints.get(key);
    }
    
    public void setInt(String key, int value) {
        ints.put(key, value);
    }
    
    public float getFloat(String key) {
        return floats.get(key);
    }
    
    public void setFloat(String key, float value) {
        floats.put(key, value);
    }
    
    public void toJobConf(JobConf job) {
        for(String key: strings.keySet()) {
            job.set(key, strings.get(key));
        }
        for(String key: ints.keySet()) {
            job.setInt(key, ints.get(key));
        }
        for(String key: floats.keySet()) {
            job.setFloat(key, floats.get(key));
        }
    }
    
}
