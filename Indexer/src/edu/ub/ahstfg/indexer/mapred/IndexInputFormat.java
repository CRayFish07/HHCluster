package edu.ub.ahstfg.indexer.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.index.IndexRecord;

public class IndexInputFormat extends FileInputFormat<Text, IndexRecord> {

    @Override
    public RecordReader<Text, IndexRecord> getRecordReader(InputSplit input,
            JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(input.toString());
        return new IndexRecordReader(job, (FileSplit) input);
    }

}
