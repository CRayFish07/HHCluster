package edu.ub.ahstfg.kmeans;

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

import edu.ub.ahstfg.indexer.IndexerMapper;
import edu.ub.ahstfg.indexer.IndexerReducer;
import edu.ub.ahstfg.indexer.mapred.IndexOutputFormat;
import edu.ub.ahstfg.io.document.ParsedDocument;
import edu.ub.ahstfg.io.index.IndexRecord;

public class KmeansIteration extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(KmeansIteration.class);

    private int nIter;
    private JobConf job;

    public KmeansIteration(int nIter) {
        this.nIter = nIter;
        job = new JobConf(getConf());
    }

    @Override
    public int run(String[] args) throws Exception {
        LOG.info("Iniciating Kmeans iteration " + nIter);
        job.setJarByClass(KmeansIteration.class);

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
        job.setInputFormat(ArcInputFormat.class);
        LOG.info("Setting output format.");
        job.setOutputFormat(IndexOutputFormat.class);

        LOG.info("Setting output data types.");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IndexRecord.class);

        // TODO mapper and reducer
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

    public static int runIteration(int nIter, String[] args) {
        int res;
        try {
            res = ToolRunner.run(new Configuration(),
                    new KmeansIteration(nIter), args);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            res = -1;
        }
        return res;
    }
}
