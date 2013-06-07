package edu.ub.ahstfg.indexer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.document.ParsedDocument;
import edu.ub.ahstfg.io.index.FeatureDescriptor;
import edu.ub.ahstfg.io.index.IndexRecord;
import edu.ub.ahstfg.io.index.Index;
import edu.ub.ahstfg.utils.Utils;

/**
 * Reducer for indexer.
 * Performs a term/keyword union and filters non common and common features.
 * @author Alberto Huelamo Segura
 *
 */
public class IndexerReducer extends MapReduceBase implements
Reducer<Text, ParsedDocument, Text, IndexRecord> {
    
    public static final String REDUCER_REPORT = "Reducer report";
    
    private Index index;
    
    @Override
    public void reduce(Text key, Iterator<ParsedDocument> values,
            OutputCollector<Text, IndexRecord> output, Reporter reporter)
                    throws IOException {
        
        if (values == null) {
            return;
        }
        
        makeIndex(values, reporter);
        
        reporter.incrCounter(REDUCER_REPORT, "Document number", index.getNumDocs());
        reporter.incrCounter(REDUCER_REPORT, "Term number", index.getNumTerms());
        reporter.incrCounter(REDUCER_REPORT, "Keyword number", index.getNumKeywords());
        
        FeatureDescriptor fd = index.getFeatures();
        output.collect(new Text("<<<FeatureDescriptor>>>"), fd);
        
        Set<String> urls = index.getDocs();
        for(String url: urls) {
            output.collect(new Text(url), index.getFullDocument(url));
        }
        
        writeNumDocs(index.getNumDocs());
    }
    
    private void makeIndex(Iterator<ParsedDocument> values, Reporter reporter) {
        index = new Index();
        
        ParsedDocument pDoc;
        while (values.hasNext()) {
            pDoc = values.next();
            index.addDocument(pDoc);
        }
        
        index.filter(0.2, 0.8);
    }
    
    private void writeNumDocs(int docs) throws IOException {
        FileSystem fs = Utils.accessHDFS();
        FSDataOutputStream out = fs.create(new Path(FeatureDescriptor.NUM_DOCS_PATH));
        out.writeInt(docs);
        out.close();
    }
}
