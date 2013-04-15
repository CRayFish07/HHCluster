package test;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.ub.ahstfg.kmeans.Centroids;
import edu.ub.ahstfg.kmeans.document.DocumentCentroid;

public class CentroidsTest {
    
    private static final Logger LOG = Logger.getLogger(CentroidsTest.class);
    
    private Centroids centroids;
    
    private int K = 5;
    private int nKeywords = 10;
    private int nTerms = 100;
    
    private String dir = Centroids.CENTROIDS_DIR_PREFIX + String.valueOf(0);
    
    public CentroidsTest() {
        centroids = new Centroids(K, DocumentCentroid.class);
        LOG.info("Initializing random centroids...");
        centroids.randomDocumentInit(nKeywords, nTerms);
        LOG.info("Writting centroids to HDFS...");
        try {
            centroids.toHDFS(dir);
        } catch (IOException e) {
            System.err.println("Problem writing centroids.");
            e.printStackTrace();
        }
        DocumentCentroid dc1 = (DocumentCentroid) centroids.get(1);
        LOG.info("Centroid 1, position 1 = " + dc1.keywordVector[1]);
        LOG.info("Centroid 1, position 2 = " + dc1.keywordVector[2]);
        LOG.info("Centroid 1, position 3 = " + dc1.keywordVector[3]);
        
        LOG.info("Reading centroids from HDFS...");
        Centroids centroids2 = new Centroids(K, DocumentCentroid.class);
        try {
            centroids2.fromHDFS(dir);
        } catch (IOException e) {
            System.err.println("Problem reading centroids.");
            e.printStackTrace();
        }
        DocumentCentroid dc2 = (DocumentCentroid) centroids2.get(1);
        LOG.info("Centroid 1, position 1 = " + dc2.keywordVector[1]);
        LOG.info("Centroid 1, position 2 = " + dc2.keywordVector[2]);
        LOG.info("Centroid 1, position 3 = " + dc2.keywordVector[3]);
        LOG.info("DONE!");
    }
    
    public static void main(String[] args) {
        new CentroidsTest();
    }
    
}
