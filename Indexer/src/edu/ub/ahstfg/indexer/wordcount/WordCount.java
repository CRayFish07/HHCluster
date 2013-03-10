package edu.ub.ahstfg.indexer.wordcount;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class WordCount extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(WordCount.class);

    private String inputPath;
    private String outputPath;

    public WordCount(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    @Override
    public int run(String[] args) throws Exception {

        LOG.info("Creating Hadoop job.");
        JobConf job = new JobConf(getConf());
        job.setJarByClass(WordCount.class);

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
        job.setInputFormat(TextInputFormat.class);
        LOG.info("Setting output format.");
        job.setOutputFormat(TextOutputFormat.class);

        LOG.info("Setting output data types.");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        LOG.info("Setting mapper and reducer.");
        job.setMapperClass(WordCountTextInputMapper.class);
        job.setReducerClass(LongSumReducer.class);

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
            res = ToolRunner.run(new Configuration(), new WordCount(inputPath,
                    outputPath), args);
            System.exit(res);
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

}
