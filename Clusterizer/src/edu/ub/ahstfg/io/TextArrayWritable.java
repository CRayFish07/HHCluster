package edu.ub.ahstfg.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

@Deprecated
public class TextArrayWritable extends ArrayWritable {

    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(Text[] values) {
        super(Text.class, values);
    }

    public TextArrayWritable(String[] values) {
        super(Text.class);
        Text[] vt = new Text[values.length];
        for (int i = 0; i < values.length; i++) {
            vt[i] = new Text(values[i]);
        }
        super.set(vt);
    }

}
