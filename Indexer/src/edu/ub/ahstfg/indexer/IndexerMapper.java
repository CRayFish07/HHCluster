package edu.ub.ahstfg.indexer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.hadoop.mapred.ArcRecord;

import edu.ub.ahstfg.io.Document;

public class IndexerMapper extends MapReduceBase implements
        Mapper<Text, ArcRecord, Text, Document> {

    @Override
    public void map(Text key, ArcRecord value,
            OutputCollector<Text, Document> output, Reporter reporter)
            throws IOException {

    }

}
