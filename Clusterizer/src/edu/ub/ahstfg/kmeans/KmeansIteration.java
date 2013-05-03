package edu.ub.ahstfg.kmeans;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.ub.ahstfg.hadoop.ParamSet;
import edu.ub.ahstfg.indexer.mapred.IndexInputFormat;
import edu.ub.ahstfg.io.DocumentDistance;

public class KmeansIteration extends Configured implements Tool {
    
    public static class SampleFilter implements PathFilter {
        
        private static int count =         0;
        private static int max   = 999999999;
        
        @Override
        public boolean accept(Path path) {
            
            if (path.getName().startsWith("_")) {
                return false;
            }
            
            SampleFilter.count++;
            
            if (SampleFilter.count > SampleFilter.max) {
                return false;
            }
            
            return true;
        }
    }
    
    private static final Logger LOG = Logger.getLogger(KmeansIteration.class);
    
    private int nIter;
    private JobConf job;
    
    private String inputPath;
    private String outputPath;
    
    private ParamSet params;
    
    public KmeansIteration(int nIter, String inputPath, String outputPath, ParamSet args) {
        this.nIter = nIter;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.params = args;
        LOG.info("Iteration " + nIter + " > Iterarion created.");
    }
    
    @Override
    public int run(String[] args) throws IOException, URISyntaxException {
        job = new JobConf(getConf());
        params.toJobConf(job);
        job.setInt(ParamSet.N_ITERATION, nIter);
        
        LOG.info("Iteration " + nIter + " > Iniciating Kmeans iteration " + nIter);
        job.setJarByClass(KmeansIteration.class); // TODO may change for Clusterizer.class
        
        LOG.info("Iteration " + nIter + " > Setting input path to '" + inputPath + "'");
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileInputFormat.setInputPathFilter(job, SampleFilter.class);
        
        LOG.info("Iteration " + nIter + " > Clearing the output path at '" + outputPath + "'");
        FileSystem fs = FileSystem.get(new URI(outputPath), job);
        
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        
        LOG.info("Iteration " + nIter + " > Setting output path to '" + outputPath + "'");
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileOutputFormat.setCompressOutput(job, false);
        
        LOG.info("Iteration " + nIter + " > Setting input format.");
        job.setInputFormat(IndexInputFormat.class);
        LOG.info("Iteration " + nIter + " > Setting output format.");
        job.setOutputFormat(TextOutputFormat.class);
        
        LOG.info("Iteration " + nIter + " > Setting output data types.");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        LOG.info("Iteration " + nIter + " > Setting mapper and reducer.");
        job.setMapperClass(KmeansMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DocumentDistance.class);
        job.setReducerClass(KmeansReducer.class);
        
        LOG.info("Iteration " + nIter + " > Running job...");
        boolean done = JobClient.runJob(job).isSuccessful();
        LOG.info("Iteration " + nIter + " > Job done.");
        if (done) {
            return 0;
        } else {
            return 1;
        }
    }
    
    public static int runIteration(int nIter, String inputPath,
            String outputPath, ParamSet args) {
        int res;
        try {
            res = ToolRunner.run(new Configuration(), new KmeansIteration(
                    nIter, inputPath, outputPath, args), new String[] { "" });
        } catch (Exception e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            res = -1;
        }
        return res;
    }
}
