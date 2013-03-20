package edu.ub.ahstfg.io.features;

import java.util.ArrayList;
import java.util.List;

import edu.ub.ahstfg.io.TextArrayWritable;

public class DocumentDescriptor {

    private List<FeatureDescriptor> features;

    public DocumentDescriptor() {
        features = new ArrayList<FeatureDescriptor>();
    }

    public void addFeature(FeatureDescriptor f) {
        features.add(f);
    }

    public FeatureDescriptor[] getFeatures() {
        return (FeatureDescriptor[]) features.toArray();
    }

    public static DocumentDescriptor defaultDescriptor() {
        DocumentDescriptor ret = new DocumentDescriptor();
        ret.addFeature(new FeatureDescriptor("terms", TextArrayWritable.class,
                0.5));
        ret.addFeature(new FeatureDescriptor("keywords",
                TextArrayWritable.class, 0.5));
        return ret;
    }

    public List<DocumentFeature> getFeatureList()
            throws InstantiationException, IllegalAccessException {
        List<DocumentFeature> ret = new ArrayList<DocumentFeature>();
        DocumentFeature f;
        for (FeatureDescriptor feature : features) {
            f = new DocumentFeature(feature.name, feature.weight);
            f.setInstance(feature.fClass.newInstance());
            ret.add(f);
        }
        return ret;
    }

}
