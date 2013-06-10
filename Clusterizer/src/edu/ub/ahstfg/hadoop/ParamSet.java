/*
 * ParamSet.java is part of HHCluster.
 *
 * HHCluster is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * HHCluster is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with HHCluster.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.ub.ahstfg.hadoop;

import java.util.HashMap;

import org.apache.hadoop.mapred.JobConf;

/**
 * Parameter storage. It is ready to transfer the parameters to Hadoop job.
 * @author Alberto Huelamo Segura
 */
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
    public static final String INPUT_PATH          = "idx_output";
    public static final String OUTPUT_PATH         = "kmeans_output";
    
    private HashMap<String, String>  strings;
    private HashMap<String, Integer> ints;
    private HashMap<String, Float>   floats;
    
    /**
     * Sole constructor.
     */
    public ParamSet() {
        strings = new HashMap<String, String>();
        ints    = new HashMap<String, Integer>();
        floats  = new HashMap<String, Float>();
    }
    
    /**
     * Gets a String parameter.
     * @param key Parameter key.
     * @return String parameter.
     */
    public String getString(String key) {
        return strings.get(key);
    }
    
    /**
     * Sets a String parameter.
     * @param key Parameter key.
     * @param value Parameter value.
     */
    public void setString(String key, String value) {
        strings.put(key, value);
    }
    
    /**
     * Gets an integer parameter.
     * @param key Parameter key.
     * @return String parameter.
     */
    public int getInt(String key) {
        return ints.get(key);
    }
    
    /**
     * Sets an integer parameter.
     * @param key Parameter key.
     * @param value Parameter value.
     */
    public void setInt(String key, int value) {
        ints.put(key, value);
    }
    
    /**
     * Gets a float parameter.
     * @param key Parameter key.
     * @return String parameter.
     */
    public float getFloat(String key) {
        return floats.get(key);
    }
    
    /**
     * Sets a float parameter.
     * @param key Parameter key.
     * @param value Parameter value.
     */
    public void setFloat(String key, float value) {
        floats.put(key, value);
    }
    
    /**
     * Transfers the parameters to a Hadoop job.
     * @param job Job where parameters will be transfered.
     */
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
