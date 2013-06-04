package edu.ub.ahstfg.io.index;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Interface index record objects. That objects are the Indexer output.
 * @author Alberto Huelamo Segura
 */
public interface IndexRecord extends Writable {
    
    /**
     * Indicates if a record is a document record.
     * @return true if the record represents a document.
     */
    public boolean isDocument();
    
    /**
     * Writes in a stream information.
     * @param out Stream to write.
     * @throws IOException Thrown if there is a problem with the communication.
     */
    public void writeOutput(DataOutputStream out) throws IOException;
    
}
