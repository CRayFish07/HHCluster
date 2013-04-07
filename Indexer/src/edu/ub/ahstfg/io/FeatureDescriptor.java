package edu.ub.ahstfg.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FeatureDescriptor implements IndexRecord, Writable {

    private String[] terms;
    private String[] keywords;

    public FeatureDescriptor() {
        terms = new String[1];
        keywords = new String[1];
    }

    public FeatureDescriptor(String[] terms, String[] keywords) {
        this.terms = terms;
        this.keywords = keywords;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        ArrayWritable buffer = new ArrayWritable(Text.class);
        buffer.readFields(input);
        terms = arrayWritable2StringArray(buffer);
        buffer = new ArrayWritable(Text.class);
        buffer.readFields(input);
        keywords = arrayWritable2StringArray(buffer);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        stringArray2ArrayWritable(terms).write(output);
        stringArray2ArrayWritable(keywords).write(output);
    }

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

}
