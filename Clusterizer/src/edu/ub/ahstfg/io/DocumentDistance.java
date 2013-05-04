package edu.ub.ahstfg.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import edu.ub.ahstfg.io.index.DocumentDescriptor;


public class DocumentDistance implements Writable {
    
    public static final boolean IS_STUB = true;
    
    private DocumentDescriptor doc;
    private DoubleWritable     distance;
    
    private BooleanWritable stub;
    
    public DocumentDistance() {
        doc = new DocumentDescriptor();
        distance = new DoubleWritable(0.0);
        stub = new BooleanWritable(false);
    }
    
    public DocumentDistance(boolean stub) {
        doc = new DocumentDescriptor();
        distance = new DoubleWritable(0.0);
        this.stub = new BooleanWritable(stub);
    }
    
    public DocumentDistance(DocumentDescriptor doc, DoubleWritable distance) {
        this.doc = doc;
        this.distance = distance;
        stub = new BooleanWritable(false);
    }
    
    public DocumentDistance(DocumentDescriptor doc, double distance) {
        this.doc = doc;
        this.distance = new DoubleWritable(distance);
        stub = new BooleanWritable(false);
    }
    
    public boolean isStub() {
        return stub.get();
    }
    
    public DocumentDescriptor getDoc() {
        return doc;
    }
    
    
    public DoubleWritable getDistance() {
        return distance;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        doc.write(out);
        distance.write(out);
        stub.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        doc.readFields(in);
        distance.readFields(in);
        stub.readFields(in);
    }
    
}
