package edu.ub.ahstfg.kmeans;

import org.apache.hadoop.fs.FileSystem;

public class Centroids {

    private Centroid[] centroids;

    public Centroids(int K) {
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

    public void setRemoteCentroids(FileSystem fs) {

    }

    public void getRemoteCentroids(FileSystem fs) {

    }

}
