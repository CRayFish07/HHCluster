/*
 * IndexWriter.java is part of HHCluster.
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

package edu.ub.ahstfg.indexer.mapred;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.index.ArrayIndex;

@Deprecated
public class IndexWriter implements RecordWriter<Text, ArrayIndex> {
    
    private DataOutputStream out;
    
    public IndexWriter(DataOutputStream out) throws IOException {
        this.out = out;
        out.writeBytes("<index>\n");
    }
    
    @Override
    public void close(Reporter reporter) throws IOException {
        try {
            out.writeBytes("</index>\n");
        } finally {
            out.close();
        }
    }
    
    @Override
    public void write(Text key, ArrayIndex value) throws IOException {
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
    
    private void writeIndex(ArrayIndex index) throws IOException {
        final String[] terms = index.getTermVector();
        final String[] keywords = index.getKeywordVector();
        final String[] urls = index.getDocumentTermVector();
        final short[][] termFreqs = index.getTermFreqMatrix();
        final short[][] keywordFreqs = index.getKeywordFreqMatrix();
        out.writeBytes("  <keywords n=\"" + keywords.length + "\">\n    ");
        for (int i = 0; i < keywords.length; i++) {
            out.writeBytes(keywords[i] + ",");
        }
        out.writeBytes("\n  </keywords>\n");
        out.writeBytes("  <terms n=\"" + terms.length + "\">\n    ");
        for (int i = 0; i < terms.length; i++) {
            out.writeBytes(terms[i] + ",");
        }
        out.writeBytes("\n  </terms>\n");
        for (int i = 0; i < urls.length; i++) {
            out.writeBytes("  <document>\n");
            out.writeBytes("    <url>\n      ");
            out.writeBytes(urls[i]);
            out.writeBytes("\n    </url>\n");
            out.writeBytes("    <keyword_freq>\n      ");
            if (keywords != null) {
                for (int j = 0; j < keywords.length; j++) {
                    out.writeBytes(keywordFreqs[i][j] + ",");
                }
            }
            out.writeBytes("\n    </keyword_freq>\n");
            out.writeBytes("    <term_freq>\n      ");
            for (int j = 0; j < terms.length; j++) {
                out.writeBytes(termFreqs[i][j] + ",");
            }
            out.writeBytes("\n    </term_freq>\n");
            out.writeBytes("  </document>\n");
        }
    }
    
}
