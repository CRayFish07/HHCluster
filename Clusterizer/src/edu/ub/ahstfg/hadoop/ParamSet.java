package edu.ub.ahstfg.hadoop;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

public class ParamSet {

    public static final String JOB_PROPERTY_PREFIX = "job_";

    private Properties properties;

    public ParamSet() {
        properties = new Properties();
    }

    public void setParam(String name, String value) {

    }

    public void setJobParam(String name, String value) {
        setParam(JOB_PROPERTY_PREFIX + name, value);
    }

    public String getParam(String name) {
        return properties.getProperty(name);
    }

    public String[] getJobArgs() {
        Set<String> names = properties.stringPropertyNames();
        ArrayList<String> ret = new ArrayList<String>();
        for (String name : names) {
            if (name.indexOf(JOB_PROPERTY_PREFIX) == 0) {
                ret.add(properties.getProperty(name));
            }
        }
        return ret.toArray(new String[ret.size()]);
    }
}
