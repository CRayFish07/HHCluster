package edu.ub.ahstfg.features;

import org.apache.hadoop.io.Writable;

public class DocumentFeature<T extends Writable> {

    private String name;
    private T instance;
    private double weight;

    public DocumentFeature(String name, double weight) {
        this.name = name;
        this.weight = weight;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getInstance() {
        return instance;
    }

    public void setInstance(T instance) {
        this.instance = instance;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

}
