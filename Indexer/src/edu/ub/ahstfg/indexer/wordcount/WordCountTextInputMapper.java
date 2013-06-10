/*
 * WordCountTextInputMapper.java is part of HHCluster.
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
