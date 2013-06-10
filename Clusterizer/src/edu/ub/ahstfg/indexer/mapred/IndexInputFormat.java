/*
 * IndexInputFormat.java is part of HHCluster.
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

package edu.ub.ahstfg.indexer.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Hadoop input format to read IndexRecords.
 * @author Alberto Huelamo Segura
 */
public class IndexInputFormat extends FileInputFormat<IntWritable, ArrayWritable> {
    
    @Override
    public RecordReader<IntWritable, ArrayWritable> getRecordReader(InputSplit input,
            JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(input.toString());
        return new IndexRecordReader(job, (FileSplit) input, reporter);
    }
    
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }
    
}
