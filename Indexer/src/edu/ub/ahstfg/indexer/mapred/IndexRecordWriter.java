package edu.ub.ahstfg.indexer.mapred;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.DocumentDescriptor;
import edu.ub.ahstfg.io.FeatureDescriptor;
import edu.ub.ahstfg.io.IndexRecord;

public class IndexRecordWriter implements RecordWriter<Text, IndexRecord> {

    private DataOutputStream out;

    public IndexRecordWriter(DataOutputStream fileOut) {
        out = fileOut;
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        out.close();
    }

    @Override
    public void write(Text key, IndexRecord value) throws IOException {
        if (value.isDocument()) {
            DocumentDescriptor doc = (DocumentDescriptor) value;
            String url = doc.getUrl();
            long[] keyFreq = doc.getKeyFreq();
            long[] termFreq = doc.getTermFreq();

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
            out.writeBytes(".\n");
        } else {
            FeatureDescriptor features = (FeatureDescriptor) value;
            String[] keywords = features.getKeywords();
            String[] terms = features.getTerms();

            out.writeBytes(key.toString() + "\t");
            if (keywords != null) {
                out.writeBytes("keywords:");
                for (int i = 0; i < keywords.length; i++) {
                    out.writeBytes(keywords[i] + ",");
                }
                out.writeBytes(";");
            }
            out.writeBytes("terms:");
            for (int i = 0; i < terms.length; i++) {
                out.writeBytes(terms[i] + ",");
            }
            out.writeBytes(".\n");
        }
    }
}
