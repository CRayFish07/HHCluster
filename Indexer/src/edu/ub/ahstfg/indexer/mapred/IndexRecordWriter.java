/*
 * IndexRecordWriter.java is part of HHCluster.
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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.io.index.IndexRecord;

/**
 * Record writer for index.
 * Writes an index record for indexer job output.
 * @author Alberto Huelamo Segura
 *
 */
public class IndexRecordWriter implements RecordWriter<Text, IndexRecord> {
    
    private JobConf job;
    private DataOutputStream out;
    
    /**
     * Sole constructor.
     * @param job Hadoop job from IndexOutputFormat.
     * @param fileOut OutputStream from IndexOutputFormat.
     */
    public IndexRecordWriter(JobConf job, DataOutputStream fileOut) {
        this.job = job;
        out = fileOut;
    }
    
    /**
     * Finishes writting.
     */
    @Override
    public void close(Reporter reporter) throws IOException {
        out.close();
    }
    
    /**
     * Writes a record.
     */
    @Override
    public void write(Text key, IndexRecord value) throws IOException {
        value.writeOutput(out);
    }
}
