package edu.ub.ahstfg.mapred;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.Index;

public class IndexWriter implements RecordWriter<Text, Index> {

    private static final String utf8 = "UTF-8";

    private DataOutputStream out;

    public IndexWriter(DataOutputStream out) throws IOException {
        this.out = out;
    }

    @Override
    public void close(Reporter reporter) throws IOException {

    }

    @Override
    public synchronized void write(Text key, Index value) throws IOException {

    }

}
