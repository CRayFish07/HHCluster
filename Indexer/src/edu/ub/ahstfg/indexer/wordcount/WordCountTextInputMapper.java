package edu.ub.ahstfg.indexer.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCountTextInputMapper extends MapReduceBase implements
Mapper<LongWritable, Text, Text, LongWritable> {
    
    private final static LongWritable ONE = new LongWritable(1);
    
    @Override
    public void map(LongWritable key, Text value,
            OutputCollector<Text, LongWritable> output, Reporter reporter)
                    throws IOException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String word;
        while (tokenizer.hasMoreTokens()) {
            word = tokenizer.nextToken();
            word = word.replaceAll("[^a-zA-Z]", "");
            output.collect(new Text(word.toLowerCase()), ONE);
        }
    }
}
