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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import edu.ub.ahstfg.io.WritableConverter;
import edu.ub.ahstfg.kmeans.Centroid;
import edu.ub.ahstfg.utils.Metrics;
import edu.ub.ahstfg.utils.Utils;

/**
 * Centroid implementation for document features.
 * @author Alberto Huelamo Segura
 */
public class DocumentCentroid implements Centroid, Writable {
    
    private static final Logger LOG = Logger.getLogger(DocumentCentroid.class);
    
    public static final boolean RANDOM           = true;
    public static final int     RANDOM_MAX_RANGE = 10;
    
    private short[] keywordVector;
    private short[] termVector;
    
    private double distance; //distance from previous centroid
    
    /**
     * Default argument constructor.
     */
    public DocumentCentroid() {
        this(10, 10);
    }
    
    /**
     * Parametrized constructor.
     * @param random True for randon init.
     * @param keywords Number of keywords.
     * @param terms Number of terms.
     */
    public DocumentCentroid(boolean random, int keywords, int terms) {
        this(keywords, terms);
        if(random) {
            for (int i = 0; i < keywords; i++) {
                keywordVector[i] = Utils.randomIntRange(5, RANDOM_MAX_RANGE);
            }
            for (int i = 0; i < terms; i++) {
                termVector[i] = Utils.randomIntRange(5, RANDOM_MAX_RANGE);
            }
        }
        distance = 0.0;
    }
    
    /**
     * Parametrized constructor.
     * @param keywords Number of keywords.
     * @param terms Number of terms.
     */
    public DocumentCentroid(int keywords, int terms) {
        keywordVector = new short[keywords];
        termVector = new short[terms];
    }
    
    /**
     * Parametrized constructor.
     * @param keywordVector Keyword frequency.
     * @param termVector Term frequency.
     */
    public DocumentCentroid(short[] keywordVector, short[] termVector) {
        this.keywordVector = keywordVector;
        this.termVector = termVector;
    }
    
    /**
     * Gets keyword frequency vector.
     * @return An array with keyword frequency.
     */
    public short[] getKeywordVector() {
        return keywordVector;
    }
    
    /**
     * Gets term frequency vector.
     * @return An array with term frequency.
     */
    public short[] getTermVector() {
        return termVector;
    }
    
    @Override
    public double getDistance() {
        return distance;
    }
    
    @Override
    public void setDistance(double distance) {
        this.distance = distance;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        WritableConverter.shortArray2ArrayWritable(keywordVector).write(out);
        WritableConverter.shortArray2ArrayWritable(termVector).write(out);
        DoubleWritable dist = new DoubleWritable(distance);
        dist.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        ArrayWritable k = new ArrayWritable(IntWritable.class);
        k.readFields(in);
        keywordVector = WritableConverter.arrayWritable2ShortArray(k);
        ArrayWritable t = new ArrayWritable(IntWritable.class);
        t.readFields(in);
        termVector = WritableConverter.arrayWritable2ShortArray(t);
        DoubleWritable dist = new DoubleWritable();
        dist.readFields(in);
        distance = dist.get();
    }
    
    @Override
    public void toHDFS(Path path) throws IOException {
        FileSystem fs = Utils.accessHDFS();
        FSDataOutputStream out = fs.create(path);
        write(out);
        out.close();
    }
    
    @Override
    public void fromHDFS(Path path) throws IOException {
        FileSystem fs = Utils.accessHDFS();
        FSDataInputStream in = fs.open(path);
        readFields(in);
        in.close();
    }
    
    /**
     * Computes the distance with other centroid.
     * @param other The other centroid.
     * @param wk Keyword distance weight.
     * @param wt Term distance weight.
     * @return The distance.
     */
    public double distance(DocumentCentroid other, float wk, float wt) {
        double keyDistance = Metrics.euclideanDistance(keywordVector,
                other.keywordVector);
        double termDistance = Metrics.euclideanDistance(termVector,
                other.termVector);
        return wk * keyDistance + wt * termDistance;
    }
    
    /**
     * Computes new centroid using mean of the frequencies.
     * @param nKeywords Numeber of keywords.
     * @param nTerms Number of terms.
     * @param keys Keyword frequency assigned to centroid.
     * @param terms Term frequency assigned to centroid.
     * @return The new centroid.
     */
    public static DocumentCentroid calculateCentroid(int nKeywords, int nTerms,
            ArrayList<short[]>keys, ArrayList<short[]> terms) {
        short[] keyFreq  = new short[nKeywords];
        long sum;
        for(int j = 0; j < nKeywords; j++) {
            sum = 0;
            for(int i = 0; i < keys.size(); i++) {
                sum += keys.get(i)[j];
            }
            keyFreq[j] = (short) (sum / keys.size());
        }
        
        short[] termFreq = new short[nTerms];
        for(int j = 0; j < nTerms; j++) {
            sum = 0;
            for(int i = 0; i < terms.size(); i++) {
                sum += terms.get(i)[j];
            }
            termFreq[j] = (short) (sum / terms.size());
        }
        
        return new DocumentCentroid(keyFreq, termFreq);
    }
    
}
