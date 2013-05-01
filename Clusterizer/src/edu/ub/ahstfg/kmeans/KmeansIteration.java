package edu.ub.ahstfg.kmeans;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
    
    private static final Logger LOG = Logger.getLogger(KmeansIteration.class);
    
    private int nIter;
    private JobConf job;
    
    private String inputPath;
    private String outputPath;
    
    public KmeansIteration(int nIter, String inputPath, String outputPath, ParamSet args) {
        this.nIter = nIter;
        job = new JobConf(getConf());
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        args.toJobConf(job);
        job.setInt(ParamSet.N_ITERATION, nIter);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        LOG.info("Iniciating Kmeans iteration " + nIter);
        job.setJarByClass(KmeansIteration.class); // TODO may change for
        // Clusterizer.class
        
        // TODO input and output path
        LOG.info("Setting input path to '" + inputPath + "'");
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        
        LOG.info("Clearing the output path at '" + outputPath + "'");
        FileSystem fs = FileSystem.get(new URI(outputPath), job);
        
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        
        LOG.info("Setting output path to '" + outputPath + "'");
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileOutputFormat.setCompressOutput(job, false);
        
        // TODO input and output format
        LOG.info("Setting input format.");
        job.setInputFormat(IndexInputFormat.class);
        LOG.info("Setting output format.");
        job.setOutputFormat(TextOutputFormat.class);
        
        LOG.info("Setting output data types.");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // TODO mapper and reducer
        LOG.info("Setting mapper and reducer.");
        job.setMapperClass(KmeansMapper.class);
        job.setMapOutputValueClass(DocumentDistance.class);
        job.setReducerClass(KmeansReducer.class);
        
        if (JobClient.runJob(job).isSuccessful()) {
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
                    nIter, inputPath, outputPath, args), new String[1]);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            res = -1;
        }
        return res;
    }
}
