package edu.ub.ahstfg.indexer.mapred;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.index.IndexRecord;

/**
 * Record writer for index.
 * Writes an index record for indexer job output.
 * @author Alberto Huelamo Segura
 *
 */
public class IndexRecordWriter implements RecordWriter<Text, IndexRecord> {
    
    private JobConf job;
    private DataOutputStream out;
    
    /**
     * Sole constructor.
     * @param job Hadoop job from IndexOutputFormat.
     * @param fileOut OutputStream from IndexOutputFormat.
     */
    public IndexRecordWriter(JobConf job, DataOutputStream fileOut) {
        this.job = job;
        out = fileOut;
    }
    
    /**
     * Finishes writting.
     */
    @Override
    public void close(Reporter reporter) throws IOException {
        out.close();
    }
    
    /**
     * Writes a record.
     */
    @Override
    public void write(Text key, IndexRecord value) throws IOException {
        value.writeOutput(out);
    }
}
