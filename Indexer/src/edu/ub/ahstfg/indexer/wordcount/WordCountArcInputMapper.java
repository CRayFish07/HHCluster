/*
 * WordCountArcInputMapper.java is part of HHCluster.
 *
 * HHCluster is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * HHCluster is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with HHCluster.  If not, see <http://www.gnu.org/licenses/>.
 */

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
