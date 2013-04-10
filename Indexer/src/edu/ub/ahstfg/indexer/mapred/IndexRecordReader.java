package edu.ub.ahstfg.indexer.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import edu.ub.ahstfg.io.index.DocumentDescriptor;
import edu.ub.ahstfg.io.index.FeatureDescriptor;
import edu.ub.ahstfg.io.index.IndexRecord;
import edu.ub.ahstfg.utils.Utils;

public class IndexRecordReader implements RecordReader<Text, IndexRecord> {

    private LineRecordReader lineReader;
    private LongWritable lineKey;
    private Text lineValue;

    public IndexRecordReader(JobConf job, FileSplit input) throws IOException {
        lineReader = new LineRecordReader(job, input);
        lineKey = lineReader.createKey();
        lineValue = lineReader.createValue();
    }

    @Override
    public boolean next(Text key, IndexRecord value) throws IOException {
        if (!lineReader.next(lineKey, lineValue)) {
            return false;
        }

        String[] pair = Utils.trimStringArray(lineValue.toString().split("\t"));
        if (pair.length != 2) {
            throw new IOException("Invalid record received");
        }
        key.set(pair[0]);
        String[] keyTerms = pair[1].split(";");
        String[] keywords = keyTerms[0].split(":");
        String[] terms = keyTerms[1].split(":");

        if (!keywords[0].equals("keywords") || !terms[0].equals("terms")) {
            throw new IOException("Invalid record received");
        }

        if (pair[0].equals(FeatureDescriptor.KEY)) {
            value = new FeatureDescriptor(Utils.trimStringArray(terms[1]
                    .split(",")), Utils.trimStringArray(keywords[1].split(",")));
        } else {
            value = new DocumentDescriptor(pair[0],
                    Utils.stringArray2LongArray(Utils.trimStringArray(terms[1]
                            .split(","))), Utils.stringArray2LongArray(Utils
                            .trimStringArray(keywords[1].split(","))));
        }

        return true;
    }

    @Override
    public Text createKey() {
        return new Text("");
    }

    @Override
    public IndexRecord createValue() {
        return new DocumentDescriptor();
    }

    @Override
    public long getPos() throws IOException {
        return lineReader.getPos();
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }

    @Override
    public float getProgress() throws IOException {
        return lineReader.getProgress();
    }

}
