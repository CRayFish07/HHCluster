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

public class Indexer extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Indexer.class);

    private String inputPath;
    private String outputPath;

    public Indexer(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    @Override
    public int run(String[] arg0) throws Exception {
        LOG.info("Creating Hadoop job for Indexer.");
        JobConf job = new JobConf(getConf());
        job.setJarByClass(Indexer.class);

        LOG.info("Setting input path to '" + inputPath + "'");
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        // Set filters if it's necessary.

        LOG.info("Clearing the output path at '" + outputPath + "'");
        // Change URI to Path if it's necessary.
        FileSystem fs = FileSystem.get(new URI(outputPath), job);

        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }

        LOG.info("Setting output path to '" + outputPath + "'");
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileOutputFormat.setCompressOutput(job, false);

        LOG.info("Setting input format.");
        // job.setInputFormat(TextInputFormat.class);
        job.setInputFormat(ArcInputFormat.class);
        LOG.info("Setting output format.");
        job.setOutputFormat(IndexOutputFormat.class);

        LOG.info("Setting output data types.");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IndexRecord.class);

        LOG.info("Setting mapper and reducer.");
        // job.setMapperClass(WordCountTextInputMapper.class);
        job.setMapperClass(IndexerMapper.class);
        job.setMapOutputValueClass(ParsedDocument.class);
        job.setReducerClass(IndexerReducer.class);
        // job.setNumReduceTasks(1);

        if (JobClient.runJob(job).isSuccessful()) {
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];
        int res;
        try {
            res = ToolRunner.run(new Configuration(), new Indexer(inputPath,
                    outputPath), args);
            System.exit(res);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

}
