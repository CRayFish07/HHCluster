/*
 * KmeansReducer.java is part of HHCluster.
 *
 * HHCluster is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * HHCluster is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with HHCluster.  If not, see <http://www.gnu.org/licenses/>.
 */

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

/**
 * K-means iteration reducer.
 * Computes the new centroids.
 * @author Alberto Huelamo Segura
 */
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
        ArrayList<short[]> keys = new ArrayList<short[]>();
        ArrayList<short[]> terms = new ArrayList<short[]>();
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
