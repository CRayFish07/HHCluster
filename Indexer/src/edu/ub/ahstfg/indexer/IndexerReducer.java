package edu.ub.ahstfg.indexer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.document.ParsedDocument;
import edu.ub.ahstfg.io.index.DocumentDescriptor;
import edu.ub.ahstfg.io.index.FeatureDescriptor;
import edu.ub.ahstfg.io.index.Index;
import edu.ub.ahstfg.io.index.IndexRecord;
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
        
        String[] terms    = index.getTermVector();
        String[] keywords = index.getKeywordVector();
        
        reporter.incrCounter(REDUCER_REPORT, "Term number", terms.length);
        reporter.incrCounter(REDUCER_REPORT, "Keyword number", keywords.length);
        
        FeatureDescriptor fd = new FeatureDescriptor(terms, keywords);
        output.collect(new Text("<<<FeatureDescriptor>>>"), fd);
        
        String[] urls     = index.getDocumentTermVector();
        short[][] termFreq = index.getTermFreqMatrix();
        short[][] keyFreq  = index.getKeywordFreqMatrix();
        for (int i = 0; i < urls.length; i++) {
            output.collect(new Text(urls[i]), new DocumentDescriptor(urls[i],
                    termFreq[i], keyFreq[i]));
        }
        
        writeNumDocs(urls.length);
    }
    
    private void makeIndex(Iterator<ParsedDocument> values, Reporter reporter) {
        index = new Index(reporter);
        
        ParsedDocument pDoc;
        String url;
        HashMap<String, Short> termMap, keywordMap;
        while (values.hasNext()) {
            pDoc = values.next();
            url = pDoc.getUrl().toString();
            termMap = pDoc.getTermMap();
            if (termMap != null) {
                for (String term : termMap.keySet()) {
                    index.addTerm(term, url, termMap.get(term));
                }
            }
            keywordMap = pDoc.getKeywordMap();
            if (keywordMap != null) {
                for (String keyword : keywordMap.keySet()) {
                    index.addKeyword(keyword, url, keywordMap.get(keyword));
                }
            }
        }
        
        index.filter(0.2, 0.8);
    }
    
    private void writeNumDocs(int docs) throws IOException {
        FileSystem fs = Utils.accessHDFS();
        FSDataOutputStream out = fs.create(new Path(
                FeatureDescriptor.NUM_DOCS_PATH));
        out.writeInt(docs);
        out.close();
    }
}
