package test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSAccess {

    public static void main(String[] args) throws IOException {
        Configuration config = new Configuration();
        config.set("fs.default.name", "hdfs://127.0.0.1:9000/");
        FileSystem dfs = FileSystem.get(config);
        dfs.delete(new Path("test"), true);
        dfs.close();
    }

}
