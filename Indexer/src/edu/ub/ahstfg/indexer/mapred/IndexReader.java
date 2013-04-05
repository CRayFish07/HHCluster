package edu.ub.ahstfg.indexer.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import edu.ub.ahstfg.io.Index;

public class IndexReader implements RecordReader<Text, Index> {

    public IndexReader(JobConf job, FileSplit split) {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Text createKey() {
        return null;
    }

    @Override
    public Index createValue() {
        return null;
    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }

    @Override
    public boolean next(Text key, Index value) throws IOException {
        return false;
    }

}
