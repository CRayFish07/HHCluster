package edu.ub.ahstfg.io;

import java.io.DataOutputStream;
import java.io.IOException;

public interface IndexRecord {

    public static final boolean DOCUMENT = true;
    public static final boolean FEATURE = false;

    public boolean isDocument();

    public void writeOutput(DataOutputStream out) throws IOException;

}
