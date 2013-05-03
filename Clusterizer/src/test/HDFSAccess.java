package test;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.ub.ahstfg.io.index.FeatureDescriptor;
import edu.ub.ahstfg.utils.Utils;

public class HDFSAccess {
    
    private static final Path PATH = new Path("test_features");
    
    private static final Logger LOG = Logger.getLogger(HDFSAccess.class);
    
    public static void main(String[] args) throws IOException {
        //        Configuration config = new Configuration();
        //        config.set("fs.default.name", "hdfs://127.0.0.1:9000/");
        //        FileSystem dfs = FileSystem.get(config);
        //        dfs.delete(new Path("test"), true);
        FileSystem fs = Utils.accessHDFS();
        FSDataOutputStream out = fs.create(PATH);
        //        FeatureDescriptor fd = new FeatureDescriptor(new String[] {"ola", "ke"}, new String[] {"ase"});
        //        fd.writeOutput(out);
        //        LOG.info("WRITE: Num terms = " + fd.getTerms().length);
        //        LOG.info("WRITE: Num keywords = " + fd.getKeywords().length);
        int[] features = new int[2];
        FeatureDescriptor.getNumFeatures(features);
        LOG.info("READ: Num terms = " + features[1]);
        LOG.info("READ: Num keywords = " + features[0]);
    }
    
}
