package edu.ub.ahstfg.io.features;

import org.apache.hadoop.io.Writable;

public class FeatureDescriptor {

    String name;
    Class<? extends Writable> fClass;
    double weight;

    public FeatureDescriptor(String name, Class<? extends Writable> fClass, double weight) {
        this.name = name;
        this.fClass = fClass;
        this.weight = weight;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Class<?> getfClass() {
        return fClass;
    }

    public void setfClass(Class<? extends Writable> fClass) {
        this.fClass = fClass;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

}
