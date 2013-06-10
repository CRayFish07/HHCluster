/*
 * IndexerMapper.java is part of HHCluster.
 *
 * HHCluster is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * HHCluster is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with HHCluster.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.ub.ahstfg.indexer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.hadoop.mapred.ArcRecord;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.ub.ahstfg.io.document.ParsedDocument;
/**
 * Mapper for indexer.
 * Counts words from HTML body and keywords from meta tag.
 * @author Alberto Huelamo Segura
 *
 */
public class IndexerMapper extends MapReduceBase implements
Mapper<Text, ArcRecord, Text, ParsedDocument> {
    
    private final String _counterGroup = "Mapper report";
    private static final Text KEY = new Text("doc");
    
    @Override
    public void map(Text key, ArcRecord value,
            OutputCollector<Text, ParsedDocument> output, Reporter reporter)
                    throws IOException {
        
        try {
            
            if (!value.getContentType().contains("html")) {
                reporter.incrCounter(this._counterGroup, "Skipped - Not HTML",
                        1);
                return;
            }
            
            reporter.incrCounter(this._counterGroup,
                    "Content Type - " + value.getContentType(), 1);
            
            if (value.getContentLength() > (5 * 1024 * 1024)) {
                reporter.incrCounter(this._counterGroup,
                        "Skipped - HTML Too Long", 1);
                return;
            }
            
            Document doc = value.getParsedHTML();
            
            if (doc == null) {
                reporter.incrCounter(this._counterGroup,
                        "Skipped - Unable to Parse HTML", 1);
                return;
            }
            
            ParsedDocument pDoc = new ParsedDocument(value.getURL());
            
            String bodyText = doc.body().text();
            StringTokenizer tokenizer = new StringTokenizer(bodyText);
            String word;
            while (tokenizer.hasMoreTokens()) {
                word = tokenizer.nextToken();
                word = word.replaceAll("[^a-zA-Z]", "");
                pDoc.addTerm(word.toLowerCase().trim());
                reporter.incrCounter(_counterGroup, "Parsed terms", 1);
            }
            
            Elements metas = doc.getElementsByTag("meta");
            if (metas != null) {
                Element meta;
                String[] keywords;
                for (int i = 0; i < metas.size(); i++) {
                    meta = metas.get(i);
                    if (meta.attr("name").equals("keywords")) {
                        keywords = meta.attr("content").split(",");
                        for (String keyword : keywords) {
                            pDoc.addKeyword(keyword.toLowerCase().trim());
                            reporter.incrCounter(_counterGroup,
                                    "Parsed keywords", 1);
                        }
                    }
                }
            }
            
            output.collect(KEY, pDoc);
            
        } catch (Throwable e) {
            
            if (e.getClass().equals(OutOfMemoryError.class)) {
                System.gc();
            }
            
            reporter.incrCounter(this._counterGroup,
                    "Skipped - Exception Thrown", 1);
        }
    }
    
}
