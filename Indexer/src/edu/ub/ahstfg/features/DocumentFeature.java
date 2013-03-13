package edu.ub.ahstfg.features;

import org.apache.hadoop.io.Writable;

public class DocumentFeature {

    private String name;
    private Writable instance;
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

    public Writable getInstance() {
        return instance;
    }

    public void setInstance(Writable instance) {
        this.instance = instance;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

}
