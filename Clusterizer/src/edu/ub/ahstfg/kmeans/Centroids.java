package edu.ub.ahstfg.kmeans;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.ub.ahstfg.kmeans.document.DocumentCentroid;

/**
 * Centroid set management class.
 * @author Alberto Huelamo Segura
 */
public class Centroids {
    
    private static final Logger LOG = Logger.getLogger(Centroids.class);
    
    public static final String CENTROIDS_DIR_PREFIX  = "./cs_";
    public static final String CENTROIDS_FILE_PREFIX = "/c_";
    
    private int K;
    private Centroid[] centroids;
    
    private Class<? extends Centroid> centroidClass;
    
    /**
     * Sole constructor.
     * @param K Number of clusters/centroids.
     * @param centroidClass Centroid type.
     */
    public Centroids(int K, Class<? extends Centroid> centroidClass) {
        this.K = K;
        this.centroidClass = centroidClass;
        centroids = new Centroid[K];
    }
    
    /**
     * Gets a centroid giving their ID.
     * @param i Centroid ID.
     * @return The centroid with specified ID.
     */
    public Centroid get(int i) {
        return centroids[i];
    }
    
    /**
     * Initializes all document centroids with random numbers
     * @param keywords Number of keywords.
     * @param terms Number of terms.
     */
    public void randomDocumentInit(int keywords, int terms) {
        for (int i = 0; i < centroids.length; i++) {
            centroids[i] = new DocumentCentroid(DocumentCentroid.RANDOM,
                    keywords, terms);
        }
    }
    
    /**
     * Measures the distances between iterations and indicates if the clustering
     * is finished.
     * @return True if the centroids have not moved.
     */
    public boolean isFinished() {
        for(Centroid c: centroids) {
            LOG.info("Distance: " + c.getDistance());
            if(c.getDistance() > 0.0) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Writes centroid set in the HDFS.
     * @param dir Directory to write.
     * @throws IOException
     */
    public void toHDFS(String dir) throws IOException {
        int i = 0;
        for (Centroid c : centroids) {
            c.toHDFS(new Path(dir + CENTROIDS_FILE_PREFIX + String.valueOf(i)));
            i++;
        }
    }
    
    /**
     * Reads centroids from the HDFS.
     * @param dir Directory to read.
     * @throws IOException
     */
    public void fromHDFS(String dir)
            throws IOException {
        for (int i = 0; i < K; i++) {
            try {
                centroids[i] = centroidClass.newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            centroids[i].fromHDFS(new Path(dir + CENTROIDS_FILE_PREFIX
                    + String.valueOf(i)));
        }
    }
}
