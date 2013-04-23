package edu.ub.ahstfg.clusterizer;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.ub.ahstfg.hadoop.ParamSet;
import edu.ub.ahstfg.io.index.FeatureDescriptor;
import edu.ub.ahstfg.kmeans.Centroids;
import edu.ub.ahstfg.kmeans.KmeansIteration;
import edu.ub.ahstfg.kmeans.document.DocumentCentroid;

public class Clusterizer {
    
    private static final Logger LOG = Logger.getLogger(Clusterizer.class);
    
    private static final int W_KEYWORDS = 0;
    private static final int W_TERMS    = 1;
    
    private ParamSet params;
    
    private Centroids centroids;
    
    private int nIter;
    private boolean finish;
    
    public Clusterizer(int K, String args[]) throws IOException {
        params = new ParamSet();
        centroids = new Centroids(K, DocumentCentroid.class);
        params.setInt(ParamSet.K, K);
        
        int[] nFeatures = new int[2];
        FeatureDescriptor.getNumFeatures(nFeatures);
        params.setInt(ParamSet.NUM_KEYWORDS, nFeatures[0]);
        params.setInt(ParamSet.NUM_TERMS   , nFeatures[1]);
        centroids.randomDocumentInit(nFeatures[0], nFeatures[1]);
        
        nIter = 0;
        finish = false;
        
        //Initial parameters setting
        params.setFloat(ParamSet.WEIGHT_KEYWORDS, Float.valueOf(args[W_KEYWORDS]));
        params.setFloat(ParamSet.WEIGHT_TERMS   , Float.valueOf(args[W_TERMS]));
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
            if (res == 1) {
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
    
    public static void main(String[] args) {
        
    }
    
}
