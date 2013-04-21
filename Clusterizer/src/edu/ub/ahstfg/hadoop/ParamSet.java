package edu.ub.ahstfg.hadoop;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

public class ParamSet {
    
    public static final String JOB_PREFIX = "job_";
    
    public static final String OLD_CENTROIDS_PATH  = JOB_PREFIX + "old_centroids_path";
    public static final String NEW_CENTROIDS_PATH  = JOB_PREFIX + "new_centroids_path";
    public static final String K                   = JOB_PREFIX + "K";
    public static final String NUM_KEYWORDS        = JOB_PREFIX + "nKeywords";
    public static final String NUM_TERMS           = JOB_PREFIX + "nTerms";
    public static final String WEIGHT_KEYWORDS     = JOB_PREFIX + "wKeywords";
    public static final String WEIGHT_TERMS        = JOB_PREFIX + "wTerms";
    
    public static final String[] STRING_JOB_PARAMS = new String[] {
        OLD_CENTROIDS_PATH,
        NEW_CENTROIDS_PATH
    };
    
    public static final String[] INT_JOB_PARAMS = new String[] {
        K,
        NUM_KEYWORDS,
        NUM_TERMS
    };
    
    public static final String[] DOUBLE_JOB_PARAMS = new String[] {
        WEIGHT_KEYWORDS,
        WEIGHT_TERMS
    };
    
    public static final String INPUT_PATH  = "kmeans_input";
    public static final String OUTPUT_PATH = "kmeans_output";
    
    
    private Properties properties;
    
    public ParamSet() {
        properties = new Properties();
    }
    
    public void set(String name, String value) {
        properties.setProperty(name, value);
    }
    
    public void setInt(String name, int value) {
        set(name, Integer.toString(value));
    }
    
    public void setJob(String name, String value) {
        set(JOB_PREFIX + name, value);
    }
    
    public void setJobInt(String name, int value) {
        setJob(name, Integer.toString(value));
    }
    
    public String get(String name) {
        return properties.getProperty(name);
    }
    
    public int getInt(String name, int defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }
        try {
            String hexString = getHexDigits(valueString);
            if (hexString != null) {
                return Integer.parseInt(hexString, 16);
            }
            return Integer.parseInt(valueString);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
    
    public String[] getJobArgs() {
        Set<String> names = properties.stringPropertyNames();
        ArrayList<String> ret = new ArrayList<String>();
        for (String name : names) {
            if (name.indexOf(JOB_PREFIX) == 0) {
                ret.add(properties.getProperty(name));
            }
        }
        return ret.toArray(new String[ret.size()]);
    }
    
    private String getHexDigits(String value) {
        boolean negative = false;
        String str = value;
        String hexString = null;
        if (value.startsWith("-")) {
            negative = true;
            str = value.substring(1);
        }
        if (str.startsWith("0x") || str.startsWith("0X")) {
            hexString = str.substring(2);
            if (negative) {
                hexString = "-" + hexString;
            }
            return hexString;
        }
        return null;
    }
}
