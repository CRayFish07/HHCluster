package edu.ub.ahstfg.clusterizer;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.ub.ahstfg.hadoop.ParamSet;
import edu.ub.ahstfg.io.index.FeatureDescriptor;
import edu.ub.ahstfg.kmeans.Centroids;
import edu.ub.ahstfg.kmeans.KmeansIteration;
import edu.ub.ahstfg.kmeans.document.DocumentCentroid;
import edu.ub.ahstfg.utils.Utils;

public class Clusterizer {
    
    private static final Logger LOG = Logger.getLogger(Clusterizer.class);
    
    private static final int P_K           = 0;
    private static final int W_KEYWORDS    = 1;
    private static final int W_TERMS       = 2;
    private static final int N_MACHINES    = 3;
    
    private ParamSet params;
    
    private Centroids centroids;
    
    private int nIter;
    private boolean finish;
    
    public Clusterizer(int K, String args[]) throws IOException {
        params    = new ParamSet();
        centroids = new Centroids(K, DocumentCentroid.class);
        params.setInt(ParamSet.K, K);
        
        int nDocs = readNumDocs();
        params.setInt(ParamSet.NUM_DOCS, nDocs);
        
        int[] nFeatures = new int[2];
        FeatureDescriptor.getNumFeatures(nFeatures);
        params.setInt(ParamSet.NUM_KEYWORDS, nFeatures[0]);
        params.setInt(ParamSet.NUM_TERMS   , nFeatures[1]);
        centroids.randomDocumentInit(nFeatures[0], nFeatures[1]);
        
        nIter  = 0;
        finish = false;
        
        params.setFloat(ParamSet.WEIGHT_KEYWORDS, Float.valueOf(args[W_KEYWORDS]));
        params.setFloat(ParamSet.WEIGHT_TERMS   , Float.valueOf(args[W_TERMS]));
        
        params.setInt(ParamSet.NUM_MACHINES, Integer.valueOf(args[N_MACHINES]));
    }
    
    public void engage() {
        int res;
        String oldCentroidsDir, newCentroidsDir;
        do {
            oldCentroidsDir = Centroids.CENTROIDS_DIR_PREFIX
                    + String.valueOf(nIter);
            newCentroidsDir = Centroids.CENTROIDS_DIR_PREFIX
                    + String.valueOf(nIter + 1);
            params.setString(ParamSet.OLD_CENTROIDS_PATH, oldCentroidsDir);
            params.setString(ParamSet.NEW_CENTROIDS_PATH, newCentroidsDir);
            try {
                centroids.toHDFS(oldCentroidsDir);
            } catch (IOException e) {
                e.printStackTrace();
            }
            res = KmeansIteration.runIteration(nIter, ParamSet.INPUT_PATH,
                    ParamSet.OUTPUT_PATH, params);
            if (res == 1 || res == -1) {
                LOG.error("Error in iteration " + nIter + ". Aborting.");
                break;
            }
            try {
                centroids.fromHDFS(newCentroidsDir);
            } catch (IOException e) {
                e.printStackTrace();
            }
            nIter++;
            if (centroids.isFinished()) {
                finish = true;
            }
        } while (!finish);
        LOG.info("Finished in " + nIter + " iterations.");
    }
    
    private int readNumDocs() throws IOException {
        FileSystem fs = Utils.accessHDFS();
        FSDataInputStream in = fs.open(new Path("params"));
        int ret = in.readInt();
        in.close();
        return ret;
    }
    
    public static void main(String[] args) {
        if(args.length != 4) {
            System.err.println("Args: <K> <keywords_weight> <term_weight> <num_machines>");
            return;
        }
        int k = 0;
        try {
            k = Integer.valueOf(args[P_K]);
        } catch(NumberFormatException ex) {
            System.err.println("Args: <K> <keywords_weight> <term_weight> <num_machines>");
            System.err.println("K must be an integer.");
            return;
        }
        try {
            Clusterizer app = new Clusterizer(k, args);
            app.engage();
        } catch (IOException e) {
            System.err.println("Problem accessing HDFS.");
            e.printStackTrace();
        }
    }
    
}
