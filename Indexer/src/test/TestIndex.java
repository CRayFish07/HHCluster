package test;

import edu.ub.ahstfg.io.document.ParsedDocument;
import edu.ub.ahstfg.io.index.DocumentDescriptor;
import edu.ub.ahstfg.io.index.FeatureDescriptor;
import edu.ub.ahstfg.io.index.NewIndex;

public class TestIndex {
    
    private static final String url1 = "http://taringa.com";
    private static final String url2 = "http://google.es";
    private static final String url3 = "http://ub.edu";
    
    private NewIndex index;
    
    public TestIndex() {
        index = new NewIndex();
        
        ParsedDocument p1 = new ParsedDocument(url1);
        p1.addTerm("hola");
        p1.addTerm("buenos");
        p1.addTerm("dias");
        p1.addTerm("senor");
        p1.addKeyword("k1");
        p1.addKeyword("k2");
        p1.addKeyword("k3");
        p1.addKeyword("k4");
        
        ParsedDocument p2 = new ParsedDocument(url2);
        p2.addTerm("que");
        p2.addTerm("cuenta");
        p2.addTerm("buenos");
        p2.addKeyword("k2");
        p2.addKeyword("k5");
        
        ParsedDocument p3 = new ParsedDocument(url3);
        p3.addTerm("hola");
        p3.addTerm("hola");
        p3.addTerm("hola");
        p3.addTerm("kk");
        
        index.addDocument(p1);
        index.addDocument(p2);
        index.addDocument(p3);
        
        index.filter((double)0.0, (double)1.0);
        
        System.out.println(index.getNumDocs());
        System.out.println(index.getNumTerms());
        System.out.println(index.getNumKeywords());
        
        FeatureDescriptor fd = index.getFeatures();
        DocumentDescriptor dd1 = index.getFullDocument(url1);
        DocumentDescriptor dd2 = index.getFullDocument(url2);
        DocumentDescriptor dd3 = index.getFullDocument(url3);
    }
    
    public static void main(String[] args) {
        new TestIndex();
    }
    
}
