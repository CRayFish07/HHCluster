package test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import edu.ub.ahstfg.utils.Utils;

public class GeneralTests {

    public GeneralTests() {
        MapWritable map = new MapWritable();
        Text key = new Text("key");
        map.put(key, new LongWritable(1));
        LongWritable value = (LongWritable) map.get(key);
        value.set(2);
        System.out.println(map.get(key));

        System.out.println("--------------------------------------------");

        String str = "        hola \t   1, ke,     ase,,,,";
        String[] split = str.split("\t");
        Utils.trimStringArray(split);
        String[] ssplit;
        for (String s : split) {
            if (s.trim().equals("hola")) {
                System.out.println(">" + s + "<");
            } else {
                ssplit = s.split(",");
                Utils.trimStringArray(ssplit);
                for (String ss : ssplit) {
                    System.out.println(">" + ss + "<");
                }
            }
        }
    }

    public static void main(String[] args) {
        new GeneralTests();
    }

}
