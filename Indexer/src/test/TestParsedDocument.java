package test;

import java.util.HashMap;

import edu.ub.ahstfg.io.ParsedDocument;

public class TestParsedDocument {

    private ParsedDocument doc;

    public TestParsedDocument() {
        doc = new ParsedDocument();
        doc.addTerm("hola");
        doc.addTerm("palabra");
        doc.addTerm("hola");
        doc.addKeyword("hola");
        doc.addKeyword("palabra");
        doc.addKeyword("hola");
        doc.addKeyword("palabra");
        doc.addKeyword("ola");
        HashMap<String, Long> terms = doc.getTermFreq();
        HashMap<String, Long> keywords = doc.getKeywordFreq();
        System.out.println("Terms:");
        for (String term : terms.keySet()) {
            System.out
                    .println("Term: " + term + " -> Freq: " + terms.get(term));
        }
        System.out.println("Keywords:");
        for (String keyword : terms.keySet()) {
            System.out.println("Term: " + keyword + " -> Freq: "
                    + keywords.get(keyword));
        }
    }

    public static void main(String[] args) {
        new TestParsedDocument();
    }

}
