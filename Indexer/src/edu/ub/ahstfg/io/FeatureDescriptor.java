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
    public boolean isDocument() {
        return IndexRecord.FEATURE;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        ArrayWritable buffer = new ArrayWritable(Text.class);
        buffer.readFields(input);
        terms = WritableConverter.arrayWritable2StringArray(buffer);
        buffer = new ArrayWritable(Text.class);
        buffer.readFields(input);
        keywords = WritableConverter.arrayWritable2StringArray(buffer);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        WritableConverter.stringArray2ArrayWritable(terms).write(output);
        WritableConverter.stringArray2ArrayWritable(keywords).write(output);
    }

}
