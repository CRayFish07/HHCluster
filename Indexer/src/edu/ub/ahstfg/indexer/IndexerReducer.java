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
        HashMap<String, Long> termMap, keywordMap;
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
                    index.addTerm(keyword, url, keywordMap.get(keyword));
                }
            }
        }

        output.collect(new Text("index"), index);

    }
}
