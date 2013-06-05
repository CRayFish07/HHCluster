package test;

import edu.ub.ahstfg.io.index.Index;

public class TestIndex {
    
    private static final String url1 = "http://taringa.com";
    private static final String url2 = "http://google.es";
    private static final String url3 = "http://ub.edu";
    
    private Index index;
    
    public TestIndex() {
        index = new Index(null);
        index.addTerm("hola", url1, (short)3);
        index.addTerm("prueba", url1, (short)1);
        index.addTerm("documento", url1, (short)1);
        index.addTerm("test", url1, (short)8);
        
        index.addTerm("hola", url2, (short)5);
        index.addTerm("caracola", url2, (short)3);
        index.addTerm("que", url2, (short)3);
        index.addTerm("haces", url2, (short)3);
        
        index.addTerm("hola", url3, (short)1);
        index.addTerm("que", url3, (short)2);
        index.addTerm("prueba", url3, (short)3);
        index.addTerm("test", url3, (short)4);
        
        String[] termVector = index.getTermVector();
        for (String s : termVector) {
            System.out.print(s + " | ");
        }
        System.out.println("");
        
        String[] docVector = index.getDocumentTermVector();
        
        short[][] freqMatrix = index.getTermFreqMatrix();
        for (int i = 0; i < docVector.length; i++) {
            System.out.print(docVector[i] + ": ");
            for (int j = 0; j < termVector.length; j++) {
                System.out.print(freqMatrix[i][j] + ", ");
            }
            System.out.println("");
        }
    }
    
    public static void main(String[] args) {
        new TestIndex();
    }
    
}
