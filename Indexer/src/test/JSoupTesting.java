package test;

import java.util.ArrayList;
import java.util.StringTokenizer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class JSoupTesting {

    /**
     * @param args
     */
    public static void main(String[] args) {
        String html = "<html><head><meta name=\"keywords\" content=\"test, testing test, testing\"></head><body><p>An <a href='http://example.com/'><b>example</b></a> link.</p></body></html>";
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

        Elements metas = doc.getElementsByTag("meta");
        if (metas != null) {
            Element meta;
            String keywords;
            for (int i = 0; i < metas.size(); i++) {
                meta = metas.get(i);
                if (meta.attr("name").equals("keywords")) {
                    keywords = meta.attr("content");
                    System.out.println(keywords);
                }
            }
        }

        for (String s : output) {
            System.out.println(s);
        }
    }
}
