package test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class GeneralTests {

    public GeneralTests() {
        MapWritable map = new MapWritable();
        Text key = new Text("key");
        map.put(key, new LongWritable(1));
        LongWritable value = (LongWritable) map.get(key);
        value.set(2);
        System.out.println(map.get(key));
    }

    public static void main(String[] args) {
        new GeneralTests();
    }

}
