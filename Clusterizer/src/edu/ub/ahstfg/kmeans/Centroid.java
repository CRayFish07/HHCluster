package edu.ub.ahstfg.kmeans;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 * Interface which defines centroid methods.
 * @author Alberto Huelamo Segura
 */
public interface Centroid {
    
    /**
     * Gets the distance to last iteration centroid.
     * @return The distance to old centroid.
     */
    public double getDistance();
    
    /**
     * Sets the distance to last iteration centroid.
     * @param distance The distance to old centroid.
     */
    public void setDistance(double distance);
    
    /**
     * Writes the centroid in HDFS.
     * @param path Path to write.
     * @throws IOException
     */
    public void toHDFS(Path path) throws IOException;
    
    /**
     * Reads the centroid from the HDFS.
     * @param path Path to read.
     * @throws IOException
     */
    public void fromHDFS(Path path) throws IOException;
    
}
