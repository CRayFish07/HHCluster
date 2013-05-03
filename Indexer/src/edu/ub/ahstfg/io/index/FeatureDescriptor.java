package edu.ub.ahstfg.io.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import edu.ub.ahstfg.io.WritableConverter;
import edu.ub.ahstfg.utils.Utils;

public class FeatureDescriptor implements IndexRecord {
    
    public static final String KEY = "<<<FeatureDescriptor>>>";
    public static final String NUM_FEATURES_PATH = "n_features.param";
    public static final String NUM_DOCS_PATH = "num_docs.param";
    
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
    
    public String[] getTerms() {
        return terms;
    }
    
    public String[] getKeywords() {
        return keywords;
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
    
    @Override
    public void writeOutput(DataOutputStream out) throws IOException {
        writeNumFeatures();
        out.writeBytes(KEY + "\t");
        if (keywords != null) {
            out.writeBytes("keywords:");
            for (int i = 0; i < keywords.length; i++) {
                out.writeBytes(keywords[i] + ",");
            }
            out.writeBytes(";");
        }
        out.writeBytes("terms:");
        for (int i = 0; i < terms.length; i++) {
            out.writeBytes(terms[i] + ",");
        }
        out.writeBytes("\n");
    }
    
    public void writeNumFeatures() throws IOException {
        FileSystem fs = Utils.accessHDFS();
        FSDataOutputStream out = fs.create(new Path(NUM_FEATURES_PATH));
        out.writeInt(keywords.length);
        out.writeInt(terms.length);
        out.close();
    }
    
    public static void getNumFeatures(int[] features)
            throws IllegalArgumentException, IOException {
        if(features.length != 2) {
            throw new IllegalArgumentException("Feature array size must be 2.");
        }
        FileSystem fs = Utils.accessHDFS();
        FSDataInputStream in = fs.open(new Path(NUM_FEATURES_PATH));
        features[0] = in.readInt();
        features[1] = in.readInt();
    }
    
}
