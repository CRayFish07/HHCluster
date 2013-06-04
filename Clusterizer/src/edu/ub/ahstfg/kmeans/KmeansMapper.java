package edu.ub.ahstfg.kmeans;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.hadoop.ParamSet;
import edu.ub.ahstfg.io.DocumentDistance;
import edu.ub.ahstfg.io.index.DocumentDescriptor;
import edu.ub.ahstfg.kmeans.document.DocumentCentroid;
import edu.ub.ahstfg.utils.Metrics;

/**
 * K-means iteration mapper.
 * Assigns each document to a cluster.
 * @author Alberto Huelamo Segura
 */
public class KmeansMapper extends MapReduceBase implements
Mapper<IntWritable, ArrayWritable, IntWritable, DocumentDistance> {
    
    public static final String REPORTER_GROUP = "Mapper report";
    
    private JobConf job;
    private OutputCollector<IntWritable, DocumentDistance> output;
    private Reporter reporter;
    
    private Centroids centroids;
    
    private int[] filledCentroids;
    
    @Override
    public void configure(JobConf job) {
        this.job = job;
    }
    
    @Override
    public void map(IntWritable key, ArrayWritable value,
            OutputCollector<IntWritable, DocumentDistance> output,
            Reporter reporter) throws IOException {
        
        this.output   = output;
        this.reporter = reporter;
        
        String centroidsPath = job.get(ParamSet.OLD_CENTROIDS_PATH);
        int K = job.getInt(ParamSet.K, 0);
        if(K <= 0) {
            return;
        }
        
        int nKeywords = job.getInt(ParamSet.NUM_KEYWORDS, 0);
        //int nTerms    = job.getInt(ParamSet.NUM_TERMS   , 0);
        boolean haveKeywords = nKeywords > 0;
        float wKeywords = job.getFloat(ParamSet.WEIGHT_KEYWORDS, (float)0.5);
        float wTerms    = job.getFloat(ParamSet.WEIGHT_TERMS   , (float)0.5);
        
        centroids = new Centroids(K, DocumentCentroid.class);
        centroids.fromHDFS(centroidsPath);
        
        filledCentroids = new int[K];
        for(int i = 0; i < filledCentroids.length; i++) {
            filledCentroids[i] = 0;
        }
        
        Writable[] ws = value.get();
        DocumentDescriptor doc;
        DocumentCentroid centroid;
        double keywordDistance, termDistance, docDistance;
        double finalDistance;
        int finalCentroid;
        for(int iDoc = 0; iDoc < ws.length; iDoc++) {
            doc = (DocumentDescriptor)ws[iDoc];
            finalDistance = -1.0;
            finalCentroid = -1;
            for(int iCentroid = 0; iCentroid < K; iCentroid++) {
                centroid = (DocumentCentroid)centroids.get(iCentroid);
                termDistance = Metrics.euclideanDistance(doc.getTermFreq(),
                        centroid.getTermVector());
                if(haveKeywords) {
                    keywordDistance = Metrics.euclideanDistance(doc.getKeyFreq(),
                            centroid.getKeywordVector());
                } else {
                    keywordDistance = 0.0;
                }
                docDistance = wKeywords * keywordDistance + wTerms * termDistance;
                if(finalDistance < 0 || finalDistance > docDistance) {
                    finalDistance = docDistance;
                    finalCentroid = iCentroid;
                }
            }
            output.collect(new IntWritable(finalCentroid), new DocumentDistance(doc, finalDistance));
            fillCentroid(finalCentroid, 1);
        }
        outVoidCentroids(output);
    }
    
    private void fillCentroid(int centroid, int amount) {
        filledCentroids[centroid] += amount;
        reporter.incrCounter(REPORTER_GROUP,
                "Docs to centroid " + centroid, amount);
    }
    
    private void outVoidCentroids(OutputCollector<IntWritable, DocumentDistance> output) throws IOException {
        for(int i = 0; i < filledCentroids.length; i++) {
            if(filledCentroids[i] <= 0) {
                reporter.incrCounter(REPORTER_GROUP, "Stub centroid " + i, 1);
                output.collect(new IntWritable(i), new DocumentDistance(DocumentDistance.IS_STUB));
            }
        }
    }
    
}
