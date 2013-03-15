package test;

import java.util.ArrayList;
import java.util.StringTokenizer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class JSoupTesting {

    /**
     * @param args
     */
    public static void main(String[] args) {
        String html = "<html><body><p>An <a href='http://example.com/'><b>example</b></a> link.</p></body></html>";
        Document doc = Jsoup.parse(html);

        String text = doc.body().text();
        StringTokenizer tokenizer = new StringTokenizer(text);
        String word;
        ArrayList<String> output = new ArrayList<String>();
        while (tokenizer.hasMoreTokens()) {
            word = tokenizer.nextToken();
            word = word.replaceAll("[^a-zA-Z]", "");
            output.add(word.toLowerCase());
        }

        for (String s : output) {
            System.out.println(s);
        }
    }
}
