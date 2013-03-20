package edu.ub.ahstfg.indexer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.Document;
import edu.ub.ahstfg.io.Index;

public class IndexerReducer extends MapReduceBase implements
        Reducer<Text, Document, IntWritable, Index> {

    @Override
    public void reduce(Text key, Iterator<Document> values,
            OutputCollector<IntWritable, Index> output, Reporter reporter)
            throws IOException {

    }

}
