package test;

import edu.ub.ahstfg.utils.Utils;

public class GenTest {
    
    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            System.out.print(Utils.randomIntRange(0, 100) + " ");
        }
    }
    
}
