package edu.ub.ahstfg.io.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.ub.ahstfg.io.WritableConverter;

/**
 * Document representation. Used in read/write indexed documents.
 * @author Alberto Huelamo Segura
 */
public class DocumentDescriptor implements IndexRecord {
    
    public static final boolean IS_DOCUMENT = true;
    
    private String url;
    private short[] termFreq;
    private short[] keyFreq;
    
    /**
     * Unparametrized constructor.
     */
    public DocumentDescriptor() {
        url = "";
        termFreq = new short[1];
        keyFreq = new short[1];
    }
    
    /**
     * Parametrized constructor.
     * @param url Document URL.
     * @param termFreq Term frequency vector.
     * @param keyFreq Keyword frequency vector.
     */
    public DocumentDescriptor(String url, short[] termFreq, short[] keyFreq) {
        this.url = url;
        this.termFreq = termFreq;
        // this.keyFreq = null;
        this.keyFreq = keyFreq;
    }
    
    @Override
    public boolean isDocument() {
        return IS_DOCUMENT;
    }
    
    /**
     * Gets the document URL.
     * @return The URL of the document.
     */
    public String getUrl() {
        return url;
    }
    
    /**
     * Gets the document term frequency.
     * @return The term frequency vector of the document.
     */
    public short[] getTermFreq() {
        return termFreq;
    }
    
    /**
     * Gets the document keyword frequency.
     * @return The keyqord frequency vector of the document.
     */
    public short[] getKeyFreq() {
        return keyFreq;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        Text t = new Text();
        t.readFields(input);
        url = t.toString();
        ArrayWritable buffer = new ArrayWritable(LongWritable.class);
        buffer.readFields(input);
        termFreq = WritableConverter.arrayWritable2ShortArray(buffer);
        buffer = new ArrayWritable(LongWritable.class);
        buffer.readFields(input);
        keyFreq = WritableConverter.arrayWritable2ShortArray(buffer);
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        new Text(url).write(output);
        WritableConverter.shortArray2ArrayWritable(termFreq).write(output);
        WritableConverter.shortArray2ArrayWritable(keyFreq).write(output);
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
