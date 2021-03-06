/*
 * IndexRecordReader.java is part of HHCluster.
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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.ub.ahstfg.hadoop.ParamSet;
import edu.ub.ahstfg.io.index.DocumentDescriptor;
import edu.ub.ahstfg.io.index.FeatureDescriptor;
import edu.ub.ahstfg.utils.Utils;

/**
 * Index record reader.
 * Reads indexed document descriptors.
 * @author Alberto Huelamo Segura
 */
public class IndexRecordReader implements RecordReader<IntWritable, ArrayWritable> {
    
    private static final String REPORTER_GROUP = "IndexRecordReader report";
    
    private LineRecordReader lineReader;
    private LongWritable lineKey;
    private Text lineValue;
    private int numMachines, numDocs, qDocsPerMapper, rDocsPerMapper;
    private int[] docsPerMapper;
    
    private int iMapper;
    
    private Reporter reporter;
    
    /**
     * Sole constructor.
     * @param job Job reading the records.
     * @param input Split where are the records.
     * @param reporter Job reporter.
     * @throws IOException
     */
    public IndexRecordReader(JobConf job, FileSplit input, Reporter reporter)
            throws IOException {
        this.reporter = reporter;
        
        lineReader = new LineRecordReader(job, input);
        lineKey    = lineReader.createKey();
        lineValue  = lineReader.createValue();
        
        numMachines    = job.getInt(ParamSet.NUM_MACHINES, 10);
        numDocs        = job.getInt(ParamSet.NUM_DOCS, 1000);
        qDocsPerMapper = numDocs / numMachines;
        rDocsPerMapper = numDocs - (qDocsPerMapper * numMachines);
        
        fillDocsPerMapper();
    }
    
    private void fillDocsPerMapper() {
        iMapper = 0;
        docsPerMapper = new int[numMachines];
        for(int i = 0; i < numMachines; i++) {
            docsPerMapper[i] = qDocsPerMapper;
            if(rDocsPerMapper > 0) {
                docsPerMapper[i]++;
                rDocsPerMapper--;
            }
        }
    }
    
    @Override
    public synchronized boolean next(IntWritable key, ArrayWritable value)
            throws IOException {
        if(iMapper >= numMachines) {
            return false;
        }
        key.set(iMapper);
        int nDocs = docsPerMapper[iMapper];
        Writable[] outSet = new Writable[nDocs];
        for (int i = 0; i < nDocs; i++) {
            if (!lineReader.next(lineKey, lineValue)) {
                return false;
            }
            String[] pair = Utils.trimStringArray(lineValue.toString().split("\t"));
            if (pair.length == 2) {
                if (!pair[0].equals(FeatureDescriptor.KEY)) {
                    String[] keyTerms = pair[1].split(";");
                    String[] keywords = keyTerms[0].split(":");
                    String[] terms = keyTerms[1].split(":");
                    if (!keywords[0].equals("keywords") || !terms[0].equals("terms")) {
                        continue;
                    }
                    try {
                        outSet[i] = new DocumentDescriptor(pair[0],
                                Utils.stringArray2ShortArray(Utils.trimStringArray(terms[1]
                                        .split(","))), Utils.stringArray2ShortArray(Utils
                                                .trimStringArray(keywords[1].split(","))));
                    } catch(ArrayIndexOutOfBoundsException ex) {
                        outSet[i] = new DocumentDescriptor(pair[0],
                                Utils.stringArray2ShortArray(Utils.trimStringArray(terms[1]
                                        .split(","))), new short[0]);
                    }
                } else {
                    i--;
                }
            }
        }
        value.set(outSet);
        iMapper++;
        return true;
    }
    
    @Override
    public IntWritable createKey() {
        return new IntWritable(iMapper);
    }
    
    @Override
    public ArrayWritable createValue() {
        return new ArrayWritable(DocumentDescriptor.class);
    }
    
    @Override
    public long getPos() throws IOException {
        return lineReader.getPos();
    }
    
    @Override
    public void close() throws IOException {
        lineReader.close();
    }
    
    @Override
    public float getProgress() throws IOException {
        return lineReader.getProgress();
    }
    
}
