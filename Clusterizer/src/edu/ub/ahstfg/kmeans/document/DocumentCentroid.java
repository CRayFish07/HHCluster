package edu.ub.ahstfg.kmeans.document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import edu.ub.ahstfg.io.WritableConverter;
import edu.ub.ahstfg.kmeans.Centroid;
import edu.ub.ahstfg.utils.Utils;

public class DocumentCentroid implements Centroid, Writable {
    
    private static final Logger LOG = Logger.getLogger(DocumentCentroid.class);
    
    public static final boolean RANDOM           = true;
    public static final int     RANDOM_MAX_RANGE = 100;
    
    public long[] keywordVector;
    public long[] termVector;
    
    public DocumentCentroid() {
        this(10, 10);
    }
    
    public DocumentCentroid(boolean random, int keywords, int terms) {
        this(keywords, terms);
        if(random) {
            for (int i = 0; i < keywords; i++) {
                keywordVector[i] = Utils.randomIntRange(0, RANDOM_MAX_RANGE);
            }
            for (int i = 0; i < terms; i++) {
                termVector[i] = Utils.randomIntRange(0, RANDOM_MAX_RANGE);
            }
        }
    }
    
    public DocumentCentroid(int keywords, int terms) {
        keywordVector = new long[keywords];
        termVector = new long[terms];
    }
    
    public DocumentCentroid(long[] keywordVector, long[] termVector) {
        this.keywordVector = keywordVector;
        this.termVector = termVector;
    }
    
    public long[] getKeywordVector() {
        return keywordVector;
    }
    
    public long[] getTermVector() {
        return termVector;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        WritableConverter.longArray2ArrayWritable(keywordVector).write(out);
        WritableConverter.longArray2ArrayWritable(termVector).write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        ArrayWritable k = new ArrayWritable(LongWritable.class);
        k.readFields(in);
        keywordVector = WritableConverter.arrayWritable2LongArray(k);
        ArrayWritable t = new ArrayWritable(LongWritable.class);
        t.readFields(in);
        termVector = WritableConverter.arrayWritable2LongArray(t);
    }
    
    @Override
    public void toHDFS(Path path) throws IOException {
        FileSystem fs = Utils.accessHDFS();
        FSDataOutputStream out = fs.create(path);
        write(out);
        fs.close();
    }
    
    @Override
    public void fromHDFS(Path path) throws IOException {
        FileSystem fs = Utils.accessHDFS();
        FSDataInputStream in = fs.open(path);
        readFields(in);
        fs.close();
    }
    
    public static DocumentCentroid calculateCentroid(int nKeywords, int nTerms,
            ArrayList<long[]>keys, ArrayList<long[]> terms) {
        long[] keyFreq  = new long[nKeywords];
        long sum;
        for(int j = 0; j < nKeywords; j++) {
            sum = 0;
            for(int i = 0; i < keys.size(); i++) {
                sum += keys.get(i)[j];
            }
            keyFreq[j] = sum / keys.size();
        }
        
        long[] termFreq = new long[nTerms];
        for(int j = 0; j < nTerms; j++) {
            sum = 0;
            for(int i = 0; i < terms.size(); i++) {
                sum += terms.get(i)[j];
            }
            termFreq[j] = sum / terms.size();
        }
        
        return new DocumentCentroid(keyFreq, termFreq);
    }
    
}
