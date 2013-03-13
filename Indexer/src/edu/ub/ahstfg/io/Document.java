package edu.ub.ahstfg.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Document implements Writable {

    private static final byte N_FEATURES = 2;

    private static final Class<IntWritable> KEYWORD_CLASS = IntWritable.class;
    private ArrayWritable keywords;
    private DoubleWritable keywordsWeight;

    private static final Class<IntWritable> TERMS_CLASS = IntWritable.class;
    private ArrayWritable terms;
    private DoubleWritable termsWeight;

    public Document() {
        this(1.0 / N_FEATURES, 1.0 / N_FEATURES);
    }

    public Document(double keywordsWeight, double termsWeight) {
        keywords = new ArrayWritable(KEYWORD_CLASS);
        terms = new ArrayWritable(TERMS_CLASS);
        this.keywordsWeight = new DoubleWritable(keywordsWeight);
        this.termsWeight = new DoubleWritable(termsWeight);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        keywords.readFields(input);
        keywordsWeight.readFields(input);
        terms.readFields(input);
        termsWeight.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        keywords.write(output);
        keywordsWeight.write(output);
        terms.write(output);
        termsWeight.write(output);
    }

}
