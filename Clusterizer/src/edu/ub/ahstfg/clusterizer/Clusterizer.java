package edu.ub.ahstfg.clusterizer;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import edu.ub.ahstfg.hadoop.ParamSet;
import edu.ub.ahstfg.kmeans.Centroids;
import edu.ub.ahstfg.kmeans.KmeansIteration;
import edu.ub.ahstfg.utils.Utils;

public class Clusterizer {

    private static final Logger LOG = Logger.getLogger(Clusterizer.class);

    private static final String HDFS_HOST = "hdfs://localhost:9000/";

    private ParamSet params;

    private int K;
    private Centroids centroids;

    private int nIter;
    private boolean finish;

    private FileSystem fs;

    public Clusterizer(int K) {
        params = new ParamSet();
        this.K = K;
        centroids = new Centroids(K);
        centroids.randomInit();
        nIter = 0;
        finish = false;
        try {
            fs = Utils.accessHDFS(HDFS_HOST);
        } catch (IOException e) {
            LOG.error("Cannot access HDFS. Aborting...");
            System.exit(-1);
        }
    }

    public void engage() {
        int res;
        do {
            centroids.setRemoteCentroids(fs);
            res = KmeansIteration.runIteration(nIter, params.getJobArgs());
            if (res == 1) {
                LOG.error("Error in iteration " + nIter + ". Aborting.");
                break;
            }
            centroids.getRemoteCentroids(fs);
            if (centroids.isFinished()) {
                finish = true;
            } else {
                nIter++;
            }
        } while (!finish);
        LOG.info("Finished in " + nIter + " iterations.");
    }

    public void close() throws IOException {
        fs.close();
    }

    public static void main(String[] args) {

    }

}
