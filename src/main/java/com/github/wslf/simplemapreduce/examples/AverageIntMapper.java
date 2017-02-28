package com.github.wslf.simplemapreduce.examples;

import com.github.wslf.simplemapreduce.Mapper;
import com.github.wslf.simplemapreduce.Pair;
import java.util.ArrayList;

/**
 *
 * @author Wsl_F
 */
public class AverageIntMapper extends Mapper<ArrayList<Integer>, Integer, Pair<Integer, Integer>> {

    @Override
    public ArrayList<Pair<Integer, Pair<Integer, Integer>>> map() {
        int sum = 0;
        for (int val : input) {
            sum += val;
        }

        Pair p = new Pair(1, new Pair<>(input.size(), sum));
        ArrayList<Pair<Integer, Pair<Integer, Integer>>> res = new ArrayList<>();
        res.add(p);
        return res;
    }

}
