package edu.ub.ahstfg.indexer.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.hadoop.mapred.ArcRecord;
import org.jsoup.nodes.Document;

public class WordCountArcInputMapper extends MapReduceBase implements
        Mapper<Text, ArcRecord, Text, LongWritable> {

    private final static LongWritable ONE = new LongWritable(1);

    @Override
    public void map(Text key, ArcRecord value,
            OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {
        Document doc = value.getParsedHTML();

        String text = doc.body().text();
        StringTokenizer tokenizer = new StringTokenizer(text);
        String word;
        while (tokenizer.hasMoreTokens()) {
            word = tokenizer.nextToken();
            word = word.replaceAll("[^a-zA-Z]", "");
            output.collect(new Text(word.toLowerCase()), ONE);
        }
    }

}
