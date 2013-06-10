/*
 * DocumentDistance.java is part of HHCluster.
 *
 * HHCluster is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * HHCluster is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with HHCluster.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.ub.ahstfg.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import edu.ub.ahstfg.io.index.DocumentDescriptor;

/**
 * Using between clustering map and reduce phases for passing documents assigned
 * to centroids.
 * @author Alberto Huelamo Segura
 */
public class DocumentDistance implements Writable {
    
    public static final boolean IS_STUB = true;
    
    private DocumentDescriptor doc;
    private DoubleWritable     distance;
    
    private BooleanWritable stub;
    
    /**
     * Unparametrized constructor.
     */
    public DocumentDistance() {
        doc = new DocumentDescriptor();
        distance = new DoubleWritable(0.0);
        stub = new BooleanWritable(false);
    }
    
    /**
     * Constructor for stub documents.
     * @param stub true to make a stub docuemnt.
     */
    public DocumentDistance(boolean stub) {
        doc = new DocumentDescriptor();
        distance = new DoubleWritable(0.0);
        this.stub = new BooleanWritable(stub);
    }
    
    /**
     * Constructor specifying the document descriptor and the distance to centroid.
     * @param doc Document descriptor.
     * @param distance Distance to centroid.
     */
    public DocumentDistance(DocumentDescriptor doc, DoubleWritable distance) {
        this.doc = doc;
        this.distance = distance;
        stub = new BooleanWritable(false);
    }
    
    /**
     * Constructor specifying the document descriptor and the distance to centroid.
     * @param doc Document descriptor.
     * @param distance Distance to centroid.
     */
    public DocumentDistance(DocumentDescriptor doc, double distance) {
        this.doc = doc;
        this.distance = new DoubleWritable(distance);
        stub = new BooleanWritable(false);
    }
    
    /**
     * Indicates if this document is a stub.
     * @return True if this is a stub document.
     */
    public boolean isStub() {
        return stub.get();
    }
    
    /**
     * Gets document descriptor.
     * @return The document descriptor.
     */
    public DocumentDescriptor getDoc() {
        return doc;
    }
    
    /**
     * Gets the distance to centroid.
     * @return Distance to centroid.
     */
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
