package edu.ub.ahstfg.io;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Utils to convert standard data to writable and vice versa.
 * Those functions are useful passing objects through Hadoop phases.
 * @author Alberto Huelamo Segura
 *
 */
public class WritableConverter {
    
    /**
     * Converts a long ArrayList to ArrayWritable.
     * @param input Long ArrayList to convert.
     * @return Converted ArrayWritable.
     */
    public static ArrayWritable arrayListLong2ArrayWritable(
            ArrayList<Long> input) {
        ArrayWritable ret = new ArrayWritable(LongWritable.class);
        Writable[] ws = new Writable[input.size()];
        for (int i = 0; i < input.size(); i++) {
            ws[i] = new LongWritable(input.get(i));
        }
        ret.set(ws);
        return ret;
    }
    
    /**
     * Converts String ArrayList to ArrayWritable.
     * @param input String ArrayList to convert.
     * @return Converted ArrayWritable.
     */
    public static ArrayWritable arrayListString2ArrayWritable(
            ArrayList<String> input) {
        ArrayWritable ret = new ArrayWritable(Text.class);
        Writable[] ws = new Writable[input.size()];
        for (int i = 0; i < input.size(); i++) {
            ws[i] = new Text(input.get(i));
        }
        ret.set(ws);
        return ret;
    }
    
    /**
     * Converts long ArrayWritable to long ArrayList.
     * @param input Long ArrayWritable to convert.
     * @return Converted long ArrayList.
     */
    public static ArrayList<Long> arrayWritable2ArrayListLong(
            ArrayWritable input) {
        ArrayList<Long> ret = new ArrayList<Long>();
        Writable[] ws = input.get();
        LongWritable l;
        for (Writable w : ws) {
            l = (LongWritable) w;
            ret.add(l.get());
        }
        return ret;
    }
    
    /**
     * Converts String ArrayWritable to String ArrayList.
     * @param input String ArrayWritable to convert.
     * @return Converted String ArrayList.
     */
    public static ArrayList<String> arrayWritable2ArrayListString(
            ArrayWritable input) {
        ArrayList<String> ret = new ArrayList<String>();
        Writable[] ws = input.get();
        Text t;
        for (Writable w : ws) {
            t = (Text) w;
            ret.add(t.toString());
        }
        return ret;
    }
    
    /**
     * Converts Long ArrayWritable to long static array.
     * @param input Long ArrayWritable to convert.
     * @return Converted long static array.
     */
    public static long[] arrayWritable2LongArray(ArrayWritable input) {
        Writable[] ws = input.get();
        long[] ret = new long[ws.length];
        int i = 0;
        LongWritable t;
        for (Writable w : ws) {
            t = (LongWritable) w;
            ret[i] = t.get();
            i++;
        }
        return ret;
    }
    
    /**
     * Converts String ArrayWritable to string static array.
     * @param input String ArrayWritable to convert.
     * @return Converted String static array.
     */
    public static String[] arrayWritable2StringArray(ArrayWritable input) {
        Writable[] ws = input.get();
        String[] ret = new String[ws.length];
        int i = 0;
        Text t;
        for (Writable w : ws) {
            t = (Text) w;
            ret[i] = t.toString();
            i++;
        }
        return ret;
    }
    
    /**
     * Converts a HashMap<String, ArrayList<Long>> to MapWritable.
     * @param input HasMap to convert.
     * @return Converted MapWritable.
     */
    public static MapWritable hashMapStringArrayListLong2MapWritable(
            HashMap<String, ArrayList<Long>> input) {
        MapWritable ret = new MapWritable();
        ArrayList<Long> arl;
        for (String s : input.keySet()) {
            arl = input.get(s);
            ret.put(new Text(s), arrayListLong2ArrayWritable(arl));
        }
        return ret;
    }
    
    /**
     * Converts static long array to ArrayWritable.
     * @param input Static long array to convert.
     * @return Converted ArrayWritable.
     */
    public static ArrayWritable longArray2ArrayWritable(long[] input) {
        ArrayWritable ret = new ArrayWritable(LongWritable.class);
        LongWritable[] t = new LongWritable[input.length];
        int i = 0;
        for (long s : input) {
            t[i] = new LongWritable(s);
            i++;
        }
        ret.set(t);
        return ret;
    }
    
    /**
     * Converts MapWritable to HashMap<String, ArrayList<Long>>.
     * @param input MapWritable to convert.
     * @return Converted HashMap.
     */
    public static HashMap<String, ArrayList<Long>> mapWritable2HashMapStringArrayListLong(
            MapWritable input) {
        HashMap<String, ArrayList<Long>> ret = new HashMap<String, ArrayList<Long>>();
        Text t;
        ArrayWritable aw;
        ArrayList<Long> al;
        for (Writable w : input.keySet()) {
            t = (Text) w;
            aw = (ArrayWritable) input.get(t);
            al = arrayWritable2ArrayListLong(aw);
            ret.put(t.toString(), al);
        }
        return ret;
    }
    
    /**
     * Converts a static String array to ArrayWritable.
     * @param input Static String array to convert.
     * @return Converted ArrayWritable.
     */
    public static ArrayWritable stringArray2ArrayWritable(String[] input) {
        ArrayWritable ret = new ArrayWritable(Text.class);
        Text[] t = new Text[input.length];
        int i = 0;
        for (String s : input) {
            t[i] = new Text(s);
            i++;
        }
        ret.set(t);
        return ret;
    }
    
}
