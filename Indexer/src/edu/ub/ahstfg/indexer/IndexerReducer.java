package edu.ub.ahstfg.indexer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.ParsedDocument;
import edu.ub.ahstfg.io.Index;

public class IndexerReducer extends MapReduceBase implements
        Reducer<Text, ParsedDocument, Text, Index> {

    @Override
    public void reduce(Text key, Iterator<ParsedDocument> values,
            OutputCollector<Text, Index> output, Reporter reporter)
            throws IOException {

    }

}
