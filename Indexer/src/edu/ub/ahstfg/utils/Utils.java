package edu.ub.ahstfg.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

/**
 * Utilities.
 * @author Alberto Huelamo Segura
 */
public class Utils {
    
    private static final Logger LOG = Logger.getLogger(Utils.class);
    
    /**
     * Trim all elements of a string array.
     * @param array Array to trim.
     * @return Trimmed array.
     */
    public static String[] trimStringArray(String[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] = array[i].trim();
        }
        return array;
    }
    
    /**
     * Converts an string number array to a long array.
     * @param array String array to convert.
     * @return Converted long array.
     */
    public static short[] stringArray2ShortArray(String[] array) {
        short[] ret = new short[array.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = Short.parseShort(array[i]);
        }
        return ret;
    }
    
    /**
     * Access HDFS using default namenode host address.
     * @return The FileSystem object to access the HDFS.
     * @throws IOException
     */
    public static FileSystem accessHDFS() throws IOException {
        Configuration config = new Configuration();
        return FileSystem.get(config);
    }
    
    /**
     * Gets a random integer in a range.
     * @param min Minimum range limit.
     * @param max Maximum range limit.
     * @return A random integer in the specified range.
     */
    public static short randomIntRange(int min, int max) {
        return (short) (min + (Math.random() * ((max - min) + 1)));
    }
    
}
