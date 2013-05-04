package edu.ub.ahstfg.kmeans;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import edu.ub.ahstfg.kmeans.document.DocumentCentroid;

public class Centroids {
    
    public static final String CENTROIDS_DIR_PREFIX  = "./centroids_";
    public static final String CENTROIDS_FILE_PREFIX = "/centroid_";
    
    private int K;
    private Centroid[] centroids;
    
    private Class<? extends Centroid> centroidClass;
    
    public Centroids(int K, Class<? extends Centroid> centroidClass) {
        this.K = K;
        this.centroidClass = centroidClass;
        centroids = new Centroid[K];
    }
    
    public Centroid get(int i) {
        return centroids[i];
    }
    
    public void randomDocumentInit(int keywords, int terms) {
        for (int i = 0; i < centroids.length; i++) {
            centroids[i] = new DocumentCentroid(DocumentCentroid.RANDOM,
                    keywords, terms);
        }
    }
    
    public boolean isFinished() {
        for(Centroid c: centroids) {
            if(c.getDistance() > 20.0) {
                return false;
            }
        }
        return true;
    }
    
    public void toHDFS(String dir) throws IOException {
        int i = 0;
        for (Centroid c : centroids) {
            c.toHDFS(new Path(dir + CENTROIDS_FILE_PREFIX + String.valueOf(i)));
            i++;
        }
    }
    
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
