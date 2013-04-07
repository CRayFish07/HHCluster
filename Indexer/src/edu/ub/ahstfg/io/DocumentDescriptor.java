package edu.ub.ahstfg.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DocumentDescriptor implements IndexRecord, Writable {

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
        this.keyFreq = keyFreq;
    }

    @Override
    public boolean isDocument() {
        return IndexRecord.DOCUMENT;
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

}
