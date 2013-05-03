package edu.ub.ahstfg.io.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.ub.ahstfg.io.WritableConverter;

public class DocumentDescriptor implements IndexRecord {
    
    private String url;
    private long[] termFreq;
    private long[] keyFreq;
    
    public DocumentDescriptor() {
        url = "";
        termFreq = new long[1];
        keyFreq = new long[1];
    }
    
    public DocumentDescriptor(String url, long[] termFreq, long[] keyFreq) {
        this.url = url;
        this.termFreq = termFreq;
        // this.keyFreq = null;
        this.keyFreq = keyFreq;
    }
    
    @Override
    public boolean isDocument() {
        return IndexRecord.DOCUMENT;
    }
    
    public String getUrl() {
        return url;
    }
    
    public long[] getTermFreq() {
        return termFreq;
    }
    
    public long[] getKeyFreq() {
        return keyFreq;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        Text t = new Text();
        t.readFields(input);
        url = t.toString();
        ArrayWritable buffer = new ArrayWritable(LongWritable.class);
        buffer.readFields(input);
        termFreq = WritableConverter.arrayWritable2LongArray(buffer);
        buffer = new ArrayWritable(LongWritable.class);
        buffer.readFields(input);
        keyFreq = WritableConverter.arrayWritable2LongArray(buffer);
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        new Text(url).write(output);
        WritableConverter.longArray2ArrayWritable(termFreq).write(output);
        WritableConverter.longArray2ArrayWritable(keyFreq).write(output);
    }
    
    @Override
    public void writeOutput(DataOutputStream out) throws IOException {
        out.writeBytes(url + "\t");
        if (keyFreq != null) {
            out.writeBytes("keywords:");
            for (int i = 0; i < keyFreq.length; i++) {
                out.writeBytes(keyFreq[i] + ",");
            }
            out.writeBytes(";");
        }
        out.writeBytes("terms:");
        for (int i = 0; i < termFreq.length; i++) {
            out.writeBytes(termFreq[i] + ",");
        }
        out.writeBytes("\n");
    }
    
}
