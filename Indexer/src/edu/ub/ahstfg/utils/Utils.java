package edu.ub.ahstfg.utils;

public class Utils {

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
}
