package edu.ub.ahstfg.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Utilities.
 * @author Alberto Huelamo Segura
 */
public class Utils {
    
    /**
     * Default HDFS namenode host. Can be changed.
     */
    public static String HDFS_HOST = "hdfs://localhost:9000/";//"hdfs://161.116.52.45:9000/";
    
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
    public static long[] stringArray2LongArray(String[] array) {
        long[] ret = new long[array.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = Long.parseLong(array[i]);
        }
        return ret;
    }
    
    /**
     * Access HDFS specifying namenode host address.
     * @param host Namenode host address.
     * @return The FileSystem object to access the HDFS.
     * @throws IOException
     */
    public static FileSystem accessHDFS(String host) throws IOException {
        Configuration config = new Configuration();
        config.set("fs.default.name", host);
        return FileSystem.get(config);
    }
    
    /**
     * Access HDFS using default namenode host address.
     * @return The FileSystem object to access the HDFS.
     * @throws IOException
     */
    public static FileSystem accessHDFS() throws IOException {
        return accessHDFS(HDFS_HOST);
    }
    
    /**
     * Gets a random integer in a range.
     * @param min Minimum range limit.
     * @param max Maximum range limit.
     * @return A random integer in the specified range.
     */
    public static int randomIntRange(int min, int max) {
        return min + (int) (Math.random() * ((max - min) + 1));
    }
    
}
