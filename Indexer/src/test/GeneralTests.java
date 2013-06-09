package test;

import java.util.HashMap;


public class GeneralTests {
    
    public GeneralTests() {
        HashMap<Integer, HashMap<Integer, Short>> map = new HashMap<Integer, HashMap<Integer,Short>>();
        map.put(1, new HashMap<Integer, Short>());
        HashMap<Integer, Short> m1 = map.get(1);
        m1.put(1, (short)20);
        System.out.println(map.get(1).get(1));
        increment(m1);
        System.out.println(map.get(1).get(1));
        increment(m1);
        System.out.println(map.get(1).get(1));
        increment(m1);
        System.out.println(map.get(1).get(1));
        increment(m1);
        System.out.println(map.get(1).get(1));
        increment(m1);
        System.out.println(map.get(1).get(1));
        increment(m1);
        System.out.println(map.get(1).get(1));
    }
    
    private void increment(HashMap<Integer, Short> map) {
        map.put(1, (short)(map.get(1)+1));
    }
    
    public static void main(String[] args) {
        new GeneralTests();
    }
    
}
