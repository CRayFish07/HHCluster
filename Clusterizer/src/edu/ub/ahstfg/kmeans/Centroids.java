package edu.ub.ahstfg.kmeans;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import edu.ub.ahstfg.kmeans.document.DocumentCentroid;

public class Centroids {

    public static final String CENTROIDS_DIR_PREFIX = "./centroids_";
    public static final String CENTROIDS_FILE_PREFIX = "/centroid_";

    private int K;
    private Centroid[] centroids;

    public Centroids(int K) {
        this.K = K;
        centroids = new Centroid[K];
    }

    public Centroid get(int i) {
        return centroids[i];
    }

    public void randomInit() {
        // TODO Auto-generated method stub
    }

    public boolean isFinished() {
        return false;
    }

    public void toHDFS(String dir) throws IOException {
        int i = 0;
        for (Centroid c : centroids) {
            c.toHDFS(new Path(dir + CENTROIDS_FILE_PREFIX + String.valueOf(i)));
            i++;
        }
    }

    public void fromHDFS(String dir) throws IOException {
        for (int i = 0; i < K; i++) {
            centroids[i] = new DocumentCentroid();
            centroids[i].fromHDFS(new Path(dir + CENTROIDS_FILE_PREFIX
                    + String.valueOf(i)));
        }
    }
}
