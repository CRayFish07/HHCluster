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
    
    private ParamSet params;
    
    private Centroids centroids;
    
    private int nIter;
    private boolean finish;
    
    public Clusterizer(int K) throws IOException {
        params = new ParamSet();
        centroids = new Centroids(K, DocumentCentroid.class);
        
        int[] nFeatures = new int[2];
        FeatureDescriptor.getNumFeatures(nFeatures);
        centroids.randomDocumentInit(nFeatures[0], nFeatures[1]);
        
        nIter = 0;
        finish = false;
    }
    
    public void engage() {
        int res;
        String centroidsDir = Centroids.CENTROIDS_DIR_PREFIX
                + String.valueOf(nIter);
        do {
            try {
                centroids.toHDFS(centroidsDir);
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
                centroids.fromHDFS(centroidsDir);
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
