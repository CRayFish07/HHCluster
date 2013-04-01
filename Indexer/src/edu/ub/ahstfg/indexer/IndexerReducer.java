package edu.ub.ahstfg.indexer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.Index;
import edu.ub.ahstfg.io.ParsedDocument;

public class IndexerReducer extends MapReduceBase implements
        Reducer<Text, ParsedDocument, Text, Index> {

    @Override
    public void reduce(Text key, Iterator<ParsedDocument> values,
            OutputCollector<Text, Index> output, Reporter reporter)
            throws IOException {

        if (values == null) {
            return;
        }

        Index index = new Index();

        ParsedDocument pDoc;
        String url;
        HashMap<String, Long> termMap;
        // long iterations = 0, n_iterations = 0;
        while (values.hasNext()) {
            // iterations++;
            pDoc = values.next();
            url = pDoc.getUrl().toString();
            termMap = pDoc.getTermMap();
            if (termMap != null) {
                for (String term : termMap.keySet()) {
                    index.addTerm(term, url, termMap.get(term));
                    // n_iterations++;
                }
            }
        }

        // String[] o = index.getTermVector();
        // reporter.incrCounter("Debug", "Term vector length: ", (long)
        // o.length);
        // reporter.incrCounter("Debug", "iterations: ", iterations);
        // reporter.incrCounter("Debug", "n_iterations: ", n_iterations);

        output.collect(new Text("index"), index);

    }
}
