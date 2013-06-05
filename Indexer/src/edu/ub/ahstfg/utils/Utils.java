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
     * Default HDFS namenode host. Can be changed.
     */
    public static String HDFS_HOST = "hdfs://161.116.52.45:9000/";
    
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
     * Access HDFS specifying namenode host address.
     * @param host Namenode host address.
     * @return The FileSystem object to access the HDFS.
     * @throws IOException
     */
    public static FileSystem accessHDFS(String host) throws IOException {
        Configuration config = new Configuration();
        //config.set("fs.default.name", host);
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
    public static short randomIntRange(int min, int max) {
        return (short) (min + (Math.random() * ((max - min) + 1)));
    }
    
    /**
     * Sets the namenode address.
     * @param string
     */
    public static void setNamenodeAddress(String string) {
        HDFS_HOST = string;
        LOG.info("Namenode address setted to: " + HDFS_HOST);
    }
    
}
