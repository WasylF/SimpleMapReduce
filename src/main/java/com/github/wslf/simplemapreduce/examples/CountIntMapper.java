
package com.github.wslf.simplemapreduce.examples;

import com.github.wslf.simplemapreduce.Mapper;
import com.github.wslf.simplemapreduce.Pair;
import java.util.ArrayList;

/**
 *
 * @author Wsl_F
 */
public class CountIntMapper extends Mapper<ArrayList<Integer>, Integer, Integer> {

    @Override
    public ArrayList<Pair<Integer, Integer>> map() {
        ArrayList<Pair<Integer, Integer>> result = new ArrayList<>();
        for (int val : input) {
            result.add(new Pair<>(val, 1));
        }
        
        return result;
    }

}
