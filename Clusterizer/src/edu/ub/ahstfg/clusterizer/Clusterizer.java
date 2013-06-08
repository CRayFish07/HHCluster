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

/**
 * Clusterizer main application class.
 * Controls K-means iterations execution.
 * @author Alberto Huelamo Segura
 */
public class Clusterizer {
    
    private static final Logger LOG = Logger.getLogger(Clusterizer.class);
    
    private static final int P_K              = 0;
    private static final int W_KEYWORDS       = 1;
    private static final int W_TERMS          = 2;
    private static final int N_MACHINES       = 3;
    private static final int NAMENODE_ADDRESS = 4;
    
    private ParamSet params;
    
    private Centroids centroids;
    
    private int nIter;
    private boolean finish;
    
    /**
     * Sole constructor.
     * @param K Number of clusters.
     * @param args Rest of arguments.
     * @throws IOException
     */
    public Clusterizer(int K, String args[]) throws IOException {
        LOG.info("Initiating...");
        params    = new ParamSet();
        centroids = new Centroids(K, DocumentCentroid.class);
        params.setInt(ParamSet.K, K);
        LOG.info("K setted (" + K + ")");
        
        LOG.info("Getting number of documents...");
        int nDocs = readNumDocs();
        params.setInt(ParamSet.NUM_DOCS, nDocs);
        LOG.info("Number of documents = " + nDocs);
        
        LOG.info("Getting number of features...");
        int[] nFeatures = new int[2];
        FeatureDescriptor.getNumFeatures(nFeatures);
        params.setInt(ParamSet.NUM_KEYWORDS, nFeatures[0]);
        params.setInt(ParamSet.NUM_TERMS   , nFeatures[1]);
        LOG.info("Number of keywords = " + nFeatures[0]);
        LOG.info("Number of terms = " + nFeatures[1]);
        
        centroids.randomDocumentInit(nFeatures[0], nFeatures[1]);
        LOG.info("Random centroid set initiated.");
        
        nIter  = 0;
        finish = false;
        
        params.setFloat(ParamSet.WEIGHT_KEYWORDS, Float.valueOf(args[W_KEYWORDS]));
        params.setFloat(ParamSet.WEIGHT_TERMS   , Float.valueOf(args[W_TERMS]));
        LOG.info("Setted feature weights. wKeywords = " + args[W_KEYWORDS] + ", wTerms = " + args[W_TERMS]);
        
        params.setInt(ParamSet.NUM_MACHINES, Integer.valueOf(args[N_MACHINES]));
        LOG.info("Number of machines = " + args[N_MACHINES]);
    }
    
    /**
     * Runs clustering process.
     */
    public void engage() {
        int res;
        String oldCentroidsDir, newCentroidsDir;
        do {
            nIter++;
            LOG.info("Iteration " + nIter + " > Begin.");
            oldCentroidsDir = Centroids.CENTROIDS_DIR_PREFIX + String.valueOf(nIter);
            newCentroidsDir = Centroids.CENTROIDS_DIR_PREFIX + String.valueOf(nIter + 1);
            params.setString(ParamSet.OLD_CENTROIDS_PATH, oldCentroidsDir);
            params.setString(ParamSet.NEW_CENTROIDS_PATH, newCentroidsDir);
            LOG.info("Iteration " + nIter + " > Old centroids dir setted to " + oldCentroidsDir);
            LOG.info("Iteration " + nIter + " > New centroids dir setted to " + newCentroidsDir);
            try {
                centroids.toHDFS(oldCentroidsDir);
                LOG.info("Iteration " + nIter + " > Written old centroids.");
            } catch (IOException e) {
                LOG.error("Iteration " + nIter + " > Problem writting old centroids. Aborting...");
                e.printStackTrace();
                break;
            }
            LOG.info("Iteration " + nIter + " > Running Hadoop job...");
            res = KmeansIteration.runIteration(nIter, ParamSet.INPUT_PATH,
                    ParamSet.OUTPUT_PATH, params);
            LOG.info("Iteration " + nIter + " > Hadoop job result: " + res);
            if (res == 1 || res < 0) {
                LOG.error("Error in iteration " + nIter + " > ERR = " + res + ". Aborting...");
                break;
            }
            try {
                LOG.info("Iteration " + nIter + " > Reading new centroids.");
                centroids.fromHDFS(newCentroidsDir);
            } catch (IOException e) {
                LOG.error("Iteration " + nIter + " > Problem reading new centroids. Aborting...");
                e.printStackTrace();
                break;
            }
            LOG.info("Iteration " + nIter + " > Iteration checking...");
            if (centroids.isFinished()) {
                LOG.info("Iteration " + nIter + " > Finishing...");
                finish = true;
            } else {
                LOG.info("Iteration " + nIter + " > New iteration required.");
            }
        } while (!finish);
        LOG.info("Finished in " + nIter + " iterations.");
    }
    
    private int readNumDocs() throws IOException {
        FileSystem fs = Utils.accessHDFS();
        FSDataInputStream in = fs.open(new Path(FeatureDescriptor.NUM_DOCS_PATH));
        int ret = in.readInt();
        in.close();
        return ret;
    }
    
    public static void main(String[] args) {
        if(args.length != 5) {
            LOG.error("Args: <K> <keywords_weight> <term_weight> <num_machines> <Namenode adress>");
            return;
        }
        Utils.HDFS_HOST = args[NAMENODE_ADDRESS];
        int k = 0;
        try {
            k = Integer.valueOf(args[P_K]);
        } catch(NumberFormatException ex) {
            LOG.error("Args: <K> <keywords_weight> <term_weight> <num_machines> <Namenode adress>");
            LOG.error("K must be an integer.");
            return;
        }
        try {
            Clusterizer app = new Clusterizer(k, args);
            app.engage();
        } catch (IOException e) {
            LOG.error("Problem accessing HDFS.");
            e.printStackTrace();
        }
    }
    
}
