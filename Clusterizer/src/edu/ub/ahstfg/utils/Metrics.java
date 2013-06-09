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
