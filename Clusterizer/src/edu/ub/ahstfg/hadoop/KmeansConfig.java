package edu.ub.ahstfg.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;

public class KmeansConfig {

    public static final String CONF_FILE_PATH = "./config/config";

    private JobConf job;
    private FileSystem fs;

    public KmeansConfig(JobConf job) throws IOException {
        this.job = job;
        fs = FileSystem.get(job);
    }

    public void setParam(String name, String... strings) {

    }

    public String[] getParam(String name) {
        return null;
    }

}
