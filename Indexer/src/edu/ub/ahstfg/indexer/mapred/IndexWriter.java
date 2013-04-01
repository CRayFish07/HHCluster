package edu.ub.ahstfg.indexer.mapred;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.Index;

public class IndexWriter implements RecordWriter<Text, Index> {

    private DataOutputStream out;

    public IndexWriter(DataOutputStream out) throws IOException {
        this.out = out;
        out.writeBytes("<index>\n");
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        try {
            out.writeBytes("</index>\n");
        }
        finally {
            out.close();
        }
    }

    @Override
    public void write(Text key, Index value) throws IOException {
        boolean nullKey = key == null || (Writable) key instanceof NullWritable;
        boolean nullValue = value == null
                || (Writable) value instanceof NullWritable;

        if (nullKey && nullValue) {
            return;
        }

        // if (!nullValue) {
        writeIndex(value);
        // }

    }

    private void writeIndex(Index index) throws IOException {
        final String[] terms = index.getTermVector();
        final String[] keywords = index.getKeywordVector();
        final String[] urls = index.getDocumentTermVector();
        final long[][] termFreqs = index.getTermFreqMatrix();
        final long[][] keywordFreqs = index.getKeywordFreqMatrix();
        out.writeBytes("<terms n=\"" + terms.length + "\">\n");
        for (int i = 0; i < terms.length; i++) {
            out.writeBytes(terms[i] + ",");
        }
        out.writeBytes("</terms>\n");
        for (int i = 0; i < urls.length; i++) {
            out.writeBytes("<document>\n");
            out.writeBytes("<url>");
            out.writeBytes(urls[i]);
            out.writeBytes("</url>");
            out.writeBytes("<keyword_freq>");
            for (int j = 0; j < keywords.length; j++) {
                out.writeBytes(keywordFreqs[i][j] + ",");
            }
            out.writeBytes("</keyword_freq>\n");
            out.writeBytes("<term_freq>");
            for (int j = 0; j < terms.length; j++) {
                out.writeBytes(termFreqs[i][j] + ",");
            }
            out.writeBytes("</term_freq>\n");
            out.writeBytes("</document>\n");
        }
    }

}
