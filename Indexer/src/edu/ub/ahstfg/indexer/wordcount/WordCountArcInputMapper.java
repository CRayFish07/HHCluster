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
    private final String _counterGroup = "Custom Mapper Counters";
    
    @Override
    public void map(Text key, ArcRecord value,
            OutputCollector<Text, LongWritable> output, Reporter reporter)
                    throws IOException {
        
        try {
            
            if (!value.getContentType().contains("html")) {
                reporter.incrCounter(this._counterGroup, "Skipped - Not HTML",
                        1);
                return;
            }
            
            reporter.incrCounter(this._counterGroup,
                    "Content Type - " + value.getContentType(), 1);
            
            if (value.getContentLength() > (5 * 1024 * 1024)) {
                reporter.incrCounter(this._counterGroup,
                        "Skipped - HTML Too Long", 1);
                return;
            }
            
            Document doc = value.getParsedHTML();
            
            if (doc == null) {
                reporter.incrCounter(this._counterGroup,
                        "Skipped - Unable to Parse HTML", 1);
                return;
            }
            
            String text = doc.body().text();
            StringTokenizer tokenizer = new StringTokenizer(text);
            String word;
            while (tokenizer.hasMoreTokens()) {
                word = tokenizer.nextToken();
                word = word.replaceAll("[^a-zA-Z]", "");
                output.collect(new Text(word.toLowerCase()), ONE);
            }
            
        }
        catch (Throwable e) {
            
            if (e.getClass().equals(OutOfMemoryError.class)) {
                System.gc();
            }
            
            reporter.incrCounter(this._counterGroup,
                    "Skipped - Exception Thrown", 1);
        }
    }
}
