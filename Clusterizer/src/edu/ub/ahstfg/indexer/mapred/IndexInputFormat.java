package edu.ub.ahstfg.indexer.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class IndexInputFormat extends FileInputFormat<IntWritable, ArrayWritable> {
    
    @Override
    public RecordReader<IntWritable, ArrayWritable> getRecordReader(InputSplit input,
            JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(input.toString());
        return new IndexRecordReader(job, (FileSplit) input, reporter);
    }
    
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }
    
}
