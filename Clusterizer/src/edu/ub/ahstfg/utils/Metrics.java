/*
 * Metrics.java is part of HHCluster.
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

package edu.ub.ahstfg.utils;

/**
 * Measure and distance utilities.
 * @author Alberto Huelamo Segura
 */
public class Metrics {
    
    /**
     * Calculates euclidean distance between two vectors.
     * @param p First vector.
     * @param q Secnd vector.
     * @return The euclidean distance between vectors.
     */
    public static double euclideanDistance(short[] p, short[] q) {
        if(p.length != q.length) {
            throw new IllegalArgumentException();
        }
        int length = p.length;
        double sum = 0.0;
        for(int i = 0; i < length; i++) {
            sum += Math.pow(q[i] - p[i], 2);
        }
        return Math.sqrt(sum);
    }
    
}
