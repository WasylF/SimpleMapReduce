
package com.github.wslf.simplemapreduce.examples;

import com.github.wslf.simplemapreduce.Pair;
import com.github.wslf.simplemapreduce.Reducer;

/**
 *
 * @author Wsl_F
 */
public class CountIntReducer  extends Reducer<Integer, Integer> {

    @Override
    public Pair<Integer, Integer> reduce() {
        int sum = 0;
        for (int val : input) {
            sum += val;
        }
        return new Pair<>(key, sum);
    }

}
