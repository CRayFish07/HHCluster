/*
 * Centroid.java is part of HHCluster.
 *
 * HHCluster is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * HHCluster is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with HHCluster.  If not, see <http://www.gnu.org/licenses/>.
 */

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
