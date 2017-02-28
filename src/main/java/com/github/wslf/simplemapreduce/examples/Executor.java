package com.github.wslf.simplemapreduce.examples;

import com.github.wslf.simplemapreduce.Pair;
import java.util.ArrayList;

/**
 *
 * @author Wsl_F
 */
public class Executor {

    public static void main(String[] args) {

        int n = 20;

        ArrayList<Integer> input = generateInput(n);
        long before = System.currentTimeMillis();
        System.out.println("before: " + before);

        //maxInt(input);
        uniqueInt(input);

        long after = System.currentTimeMillis();
        System.out.println("after: " + after);
        System.out.println("time: " + (after - before));
    }

    private static ArrayList<Integer> generateInput(int n) {
        ArrayList<Integer> input = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            input.add(i);
            input.add(n - i - 1);
        }
        return input;
    }

    private static void maxInt(ArrayList<Integer> input) {
        System.out.println("maxInt");
        MapReduceListInt mapReduce = new MapReduceListInt(MaxIntMapper.class, MaxIntReducer.class, 4, 4);
        ArrayList<Pair<Integer, Integer>> res = mapReduce.process(input);
        System.out.println(res.get(0).value);

    }

    private static void uniqueInt(ArrayList<Integer> input) {
        System.out.println("UniqueInt");
        MapReduceListInt mapReduce = new MapReduceListInt(UniqueIntMapper.class, UniqueIntReducer.class, 4, 4);
        ArrayList<Pair<Integer, Integer>> res = mapReduce.process(input);
        for (Pair<Integer, Integer> p : res) {
            System.out.print(p.value + " ");
        }
        System.out.println("");
    }
}
