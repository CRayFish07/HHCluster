package edu.ub.ahstfg.kmeans;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public interface Centroid {
    
    public double getDistance();
    
    public void setDistance(double distance);
    
    public void toHDFS(Path path) throws IOException;
    
    public void fromHDFS(Path path) throws IOException;
    
}
