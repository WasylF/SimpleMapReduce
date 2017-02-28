package com.github.wslf.simplemapreduce.examples;

import com.github.wslf.simplemapreduce.Pair;
import com.github.wslf.simplemapreduce.Reducer;

/**
 *
 * @author Wsl_F
 */
public class AverageIntReducer extends Reducer<Integer, Pair<Integer, Integer>> {

    @Override
    public Pair<Integer, Pair<Integer, Integer>> reduce() {
        int totalSum = 0;
        int totalCount = 0;
        for (Pair<Integer, Integer> p : input) {
            totalCount += p.key;
            totalSum += p.value;
        }
        
        return new Pair<>(key, new Pair<>(totalCount, totalSum));
    }

}
