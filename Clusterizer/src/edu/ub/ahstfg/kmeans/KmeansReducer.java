package edu.ub.ahstfg.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.hadoop.ParamSet;
import edu.ub.ahstfg.io.DocumentDistance;
import edu.ub.ahstfg.io.index.DocumentDescriptor;
import edu.ub.ahstfg.kmeans.document.DocumentCentroid;

public class KmeansReducer extends MapReduceBase implements
Reducer<IntWritable, DocumentDistance, Text, IntWritable> {
    
    public static final String REPORTER_GROUP = "Reducer report";
    
    private JobConf job;
    
    @Override
    public void configure(JobConf job) {
        this.job = job;
    }
    
    @Override
    public void reduce(IntWritable key, Iterator<DocumentDistance> values,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
                    throws IOException {
        reporter.incrCounter(REPORTER_GROUP, "Num reducers", 1);
        int nKeywords = job.getInt(ParamSet.NUM_KEYWORDS, 0);
        int nTerms    = job.getInt(ParamSet.NUM_TERMS   , 0);
        //boolean haveKeywords = nKeywords > 0;
        
        DocumentDistance d;
        ArrayList<long[]> keys = new ArrayList<long[]>();
        ArrayList<long[]> terms = new ArrayList<long[]>();
        DocumentDescriptor doc;
        boolean stub = false, allStub = false;
        int nStub = 0, count = 0;
        while(values.hasNext()) {
            count++;
            d = values.next(); stub = d.isStub();
            if(!stub) {
                doc = d.getDoc();
                keys.add(doc.getKeyFreq());
                terms.add(doc.getTermFreq());
                output.collect(new Text(doc.getUrl()), key);
            } else {
                nStub++;
            }
        }
        
        if(nStub >= count) {
            allStub = true;
        }
        
        String centroidPath = Centroids.CENTROIDS_FILE_PREFIX + String.valueOf(key.get());
        String oldPath = job.get(ParamSet.OLD_CENTROIDS_PATH) + centroidPath;
        DocumentCentroid oldCentroid = new DocumentCentroid();
        oldCentroid.fromHDFS(new Path(oldPath));
        
        DocumentCentroid newCentroid = null;
        
        if(!allStub) {
            newCentroid = DocumentCentroid.calculateCentroid(
                    nKeywords, nTerms, keys, terms);
        } else {
            newCentroid = oldCentroid;
            reporter.incrCounter(REPORTER_GROUP, "Stub centroids", 1);
        }
        double distance = newCentroid.distance(oldCentroid,
                job.getFloat(job.get(ParamSet.WEIGHT_KEYWORDS), (float)0.5),
                job.getFloat(job.get(ParamSet.WEIGHT_TERMS), (float)0.5));
        newCentroid.setDistance(distance);
        String newPath = job.get(ParamSet.NEW_CENTROIDS_PATH) + centroidPath;
        newCentroid.toHDFS(new Path(newPath));
    }
    
}
