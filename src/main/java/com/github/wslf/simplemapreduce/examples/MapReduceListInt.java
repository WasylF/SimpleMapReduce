package com.github.wslf.simplemapreduce.examples;

import com.github.wslf.simplemapreduce.MapReduce;
import java.util.ArrayList;

/**
 *
 * @author Wsl_F
 */
public class MapReduceListInt extends MapReduce<ArrayList<Integer>, Integer, Integer>{

    public MapReduceListInt(Class maperClass, Class reducerClass, int mapersNumber, int reducersNumber) {
        super(maperClass, reducerClass, mapersNumber, reducersNumber);
    }

    @Override
    public ArrayList<Integer> getSlice(ArrayList<Integer> wholeInput, int sliceNumber) {
        int n = wholeInput.size() / mapersNumber;
        int begin = sliceNumber * n;
        int end = Math.min((sliceNumber+1)*n, wholeInput.size());
        ArrayList<Integer> slice = new ArrayList<>();
        for (int i = begin; i < end; i++) {
            slice.add(wholeInput.get(i));
        }
        
        return slice;
    }
    
}
