package edu.ub.ahstfg.indexer.mapred;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.index.IndexRecord;

public class IndexRecordWriter implements RecordWriter<Text, IndexRecord> {

    private DataOutputStream out;

    public IndexRecordWriter(DataOutputStream fileOut) {
        out = fileOut;
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        out.close();
    }

    @Override
    public void write(Text key, IndexRecord value) throws IOException {
        value.writeOutput(out);
    }
}
