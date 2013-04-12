package edu.ub.ahstfg.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class Utils {

    public static final String HDFS_HOST = "hdfs://localhost:9000/";

    public static String[] trimStringArray(String[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] = array[i].trim();
        }
        return array;
    }

    public static long[] stringArray2LongArray(String[] array) {
        long[] ret = new long[array.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = Long.parseLong(array[i]);
        }
        return ret;
    }

    public static FileSystem accessHDFS(String host) throws IOException {
        Configuration config = new Configuration();
        config.set("fs.default.name", host);
        return FileSystem.get(config);
    }

    public static FileSystem accessHDFS() throws IOException {
        return accessHDFS(HDFS_HOST);
    }

}
