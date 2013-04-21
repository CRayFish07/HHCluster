package edu.ub.ahstfg.hadoop;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

public class ParamSet {
    
    public static final String JOB_PROPERTY_PREFIX = "job_";
    
    public static final String OLD_CENTROIDS_PATH  = JOB_PROPERTY_PREFIX + "old_centroids_path";
    public static final String NEW_CENTROIDS_PATH  = JOB_PROPERTY_PREFIX + "new_centroids_path";
    public static final String K                   = JOB_PROPERTY_PREFIX + "K";
    public static final String NUM_KEYWORDS        = JOB_PROPERTY_PREFIX + "nKeywords";
    public static final String NUM_TERMS           = JOB_PROPERTY_PREFIX + "nTerms";
    public static final String WEIGHT_KEYWORDS     = JOB_PROPERTY_PREFIX + "wKeywords";
    public static final String WEIGHT_TERMS        = JOB_PROPERTY_PREFIX + "wTerms";
    
    private Properties properties;
    
    public ParamSet() {
        properties = new Properties();
    }
    
    public void setParam(String name, String value) {
        properties.setProperty(name, value);
    }
    
    public void setJobParam(String name, String value) {
        setParam(JOB_PROPERTY_PREFIX + name, value);
    }
    
    public String getParam(String name) {
        return properties.getProperty(name);
    }
    
    public String[] getJobArgs() {
        Set<String> names = properties.stringPropertyNames();
        ArrayList<String> ret = new ArrayList<String>();
        for (String name : names) {
            if (name.indexOf(JOB_PROPERTY_PREFIX) == 0) {
                ret.add(properties.getProperty(name));
            }
        }
        return ret.toArray(new String[ret.size()]);
    }
}
