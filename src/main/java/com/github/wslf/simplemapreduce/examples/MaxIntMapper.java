package com.github.wslf.simplemapreduce.examples;

import com.github.wslf.simplemapreduce.Mapper;
import com.github.wslf.simplemapreduce.Pair;
import java.util.ArrayList;

/**
 *
 * @author Wsl_F
 */
public class MaxIntMapper extends Mapper<ArrayList<Integer>, Integer, Integer> {

    @Override
    public ArrayList<Pair<Integer, Integer>> map() {
        ArrayList<Pair<Integer, Integer>> result = new ArrayList<>();
        int MaxVal = input.get(0);
        for (int val : input) {
            if (val > MaxVal) {
                MaxVal = val;
            }
        }

        result.add(new Pair(1, MaxVal));
        return result;
    }

}
