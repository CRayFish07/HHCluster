package edu.ub.ahstfg.indexer;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.hadoop.mapred.ArcInputFormat;

import edu.ub.ahstfg.indexer.mapred.IndexOutputFormat;
import edu.ub.ahstfg.io.document.ParsedDocument;
import edu.ub.ahstfg.io.index.IndexRecord;

/**
 * Indexer launch class.
 * This class sets up Hadoop environment to indexer job execution.
 * @author Albert Huelamo Segura
 *
 */
public class Indexer extends Configured implements Tool {
    
    private static final Logger LOG = Logger.getLogger(Indexer.class);
    
    public static final String INPUT_PATH  = "idx_input";
    public static final String OUTPUT_PATH = "idx_output";
    
    public Indexer() {}
    
    @Override
    public int run(String[] arg0) throws Exception {
        LOG.info("Creating Hadoop job for Indexer.");
        JobConf job = new JobConf(getConf());
        job.setJarByClass(Indexer.class);
        
        LOG.info("Setting input path to '" + INPUT_PATH + "'");
        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        // Set filters if it's necessary.
        
        LOG.info("Clearing the output path at '" + OUTPUT_PATH + "'");
        // Change URI to Path if it's necessary.
        FileSystem fs = FileSystem.get(new URI(OUTPUT_PATH), job);
        
        if (fs.exists(new Path(OUTPUT_PATH))) {
            fs.delete(new Path(OUTPUT_PATH), true);
        }
        
        LOG.info("Setting output path to '" + OUTPUT_PATH + "'");
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        FileOutputFormat.setCompressOutput(job, false);
        
        LOG.info("Setting input format.");
        job.setInputFormat(ArcInputFormat.class);
        LOG.info("Setting output format.");
        job.setOutputFormat(IndexOutputFormat.class);
        
        LOG.info("Setting output data types.");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IndexRecord.class);
        
        LOG.info("Setting mapper and reducer.");
        job.setMapperClass(IndexerMapper.class);
        job.setMapOutputValueClass(ParsedDocument.class);
        job.setReducerClass(IndexerReducer.class);
        
        if (JobClient.runJob(job).isSuccessful()) {
            return 0;
        } else {
            return 1;
        }
    }
    
    public static void main(String[] args) {
        int res;
        try {
            res = ToolRunner.run(new Configuration(), new Indexer(), args);
            System.exit(res);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
    
}
