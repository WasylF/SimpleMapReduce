
package com.github.wslf.simplemapreduce.examples;

import com.github.wslf.simplemapreduce.Pair;
import com.github.wslf.simplemapreduce.Reducer;

/**
 *
 * @author Wsl_F
 */
public class UniqueIntReducer extends Reducer<Integer, Integer>{

    @Override
    public Pair<Integer, Integer> reduce() {
        return new Pair<>(1, key);
    }

}
