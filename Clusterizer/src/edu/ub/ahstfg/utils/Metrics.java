package edu.ub.ahstfg.utils;


public class Metrics {
    
    public static double euclideanDistance(long[] p, long[] q) {
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
