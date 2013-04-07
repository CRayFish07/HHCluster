package edu.ub.ahstfg.indexer.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import edu.ub.ahstfg.io.IndexRecord;

public class IndexOutputFormat extends FileOutputFormat<Text, IndexRecord> {

    @Override
    public RecordWriter<Text, IndexRecord> getRecordWriter(FileSystem ignored,
            JobConf job, String name, Progressable progress) throws IOException {
        Path file = FileOutputFormat.getTaskOutputPath(job, name);
        FileSystem fs = file.getFileSystem(job);
        FSDataOutputStream fileOut = fs.create(file, progress);
        return new IndexRecordWriter(fileOut);
    }

}
