/*
 * FeatureDescriptor.java is part of HHCluster.
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

package edu.ub.ahstfg.io.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import edu.ub.ahstfg.io.WritableConverter;
import edu.ub.ahstfg.utils.Utils;

/**
 * Representation of the features. It is used to store which keywords and terms
 * are in the index.
 * @author Alberto Huelamo Segura
 */
public class FeatureDescriptor implements IndexRecord {
    
    public static final boolean IS_FEATURE = false;
    
    public static final String KEY = "<<<FeatureDescriptor>>>";
    public static final String NUM_FEATURES_PATH = "n_features.param";
    public static final String NUM_DOCS_PATH = "num_docs.param";
    
    private String[] terms;
    private String[] keywords;
    
    /**
     * Unparametrized constructor.
     */
    public FeatureDescriptor() {
        terms = new String[1];
        keywords = new String[1];
    }
    
    /**
     * Parametrized constructor.
     * @param terms Term list.
     * @param keywords Keyword list.
     */
    public FeatureDescriptor(String[] terms, String[] keywords) {
        this.terms = terms;
        this.keywords = keywords;
    }
    
    @Override
    public boolean isDocument() {
        return IS_FEATURE;
    }
    
    /**
     * Gets the term list.
     * @return A term vector.
     */
    public String[] getTerms() {
        return terms;
    }
    
    /**
     * Gets the keyword list.
     * @return A keyword vector.
     */
    public String[] getKeywords() {
        return keywords;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        ArrayWritable buffer = new ArrayWritable(Text.class);
        buffer.readFields(input);
        terms = WritableConverter.arrayWritable2StringArray(buffer);
        buffer = new ArrayWritable(Text.class);
        buffer.readFields(input);
        keywords = WritableConverter.arrayWritable2StringArray(buffer);
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        WritableConverter.stringArray2ArrayWritable(terms).write(output);
        WritableConverter.stringArray2ArrayWritable(keywords).write(output);
    }
    
    @Override
    public void writeOutput(DataOutputStream out) throws IOException {
        writeNumFeatures();
        out.writeBytes(KEY + "\t");
        if (keywords != null) {
            out.writeBytes("keywords:");
            for (int i = 0; i < keywords.length; i++) {
                out.writeBytes(keywords[i] + ",");
            }
            out.writeBytes(";");
        }
        out.writeBytes("terms:");
        for (int i = 0; i < terms.length; i++) {
            out.writeBytes(terms[i] + ",");
        }
        out.writeBytes("\n");
    }
    
    /**
     * Writes in the HDFS the number of features.
     */
    public void writeNumFeatures() throws IOException {
        FileSystem fs = Utils.accessHDFS();
        FSDataOutputStream out = fs.create(new Path(NUM_FEATURES_PATH));
        out.writeInt(keywords.length);
        out.writeInt(terms.length);
        out.close();
    }
    
    /**
     * Reads from the HDFS the number of features.
     * @param features 2 position integer array to store the number of features.
     * @throws IllegalArgumentException Thrown if the vector has not length 2.
     * @throws IOException Thrown if there is a problem with communication.
     */
    public static void getNumFeatures(int[] features)
            throws IllegalArgumentException, IOException {
        if(features.length != 2) {
            throw new IllegalArgumentException("Feature array size must be 2.");
        }
        FileSystem fs = Utils.accessHDFS();
        FSDataInputStream in = fs.open(new Path(NUM_FEATURES_PATH));
        features[0] = in.readInt();
        features[1] = in.readInt();
    }
    
}
