package edu.ub.ahstfg.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

import edu.ub.ahstfg.io.Index;

public class IndexOutputFormat extends TextOutputFormat<Text, Index> {

    @Override
    public RecordWriter<Text, Index> getRecordWriter(FileSystem ignored,
            JobConf job, String name, Progressable progress) throws IOException {
        Path file = getOutputPath(job);
        FileSystem fs = file.getFileSystem(job);
        FSDataOutputStream fileOut = fs.create(file, progress);
        return new IndexWriter(fileOut);
    }

}
