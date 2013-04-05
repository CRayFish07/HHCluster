package edu.ub.ahstfg.indexer.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.Index;

public class IndexInputFormat extends FileInputFormat<Text, Index> {

    @Override
    public RecordReader<Text, Index> getRecordReader(InputSplit arg0,
            JobConf arg1, Reporter arg2) throws IOException {
        return null;
    }

}
