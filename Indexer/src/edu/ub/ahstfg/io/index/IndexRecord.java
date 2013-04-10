package edu.ub.ahstfg.io.index;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public interface IndexRecord extends Writable {

    public static final boolean DOCUMENT = true;
    public static final boolean FEATURE = false;

    public boolean isDocument();

    public void writeOutput(DataOutputStream out) throws IOException;

}
