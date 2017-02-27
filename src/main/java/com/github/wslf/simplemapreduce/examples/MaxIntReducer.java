package com.github.wslf.simplemapreduce.examples;

import com.github.wslf.simplemapreduce.Pair;
import com.github.wslf.simplemapreduce.Reducer;

/**
 *
 * @author Wsl_F
 */
public class MaxIntReducer extends Reducer<Integer, Integer> {
    
    @Override
    public Pair<Integer, Integer> reduce() {
        int maxVal = input.get(0);
        for (int val : input) {
            if (val > maxVal) {
                maxVal = val;
            }
        }

        return new Pair<>(key, maxVal);
    }

}
