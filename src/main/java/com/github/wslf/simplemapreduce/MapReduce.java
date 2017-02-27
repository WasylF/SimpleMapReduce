package com.github.wslf.simplemapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *
 * @author Wsl_F
 */
public abstract class MapReduce<InputT, KeyT, ValueT> {

    protected final Class maperClass;
    protected final Class reducerClass;

    protected final int mapersNumber;
    protected final int reducersNumber;

    public MapReduce(Class maperClass, Class reducerClass, int mapersNumber, int reducersNumber) {
        this.maperClass = maperClass;
        this.reducerClass = reducerClass;
        this.mapersNumber = mapersNumber;
        this.reducersNumber = reducersNumber;
    }

    public ArrayList<Pair<KeyT, ValueT>> process(InputT input) {
        return reduce(map(input));
    }

    /**
     *
     * @param wholeInput whole input data
     * @param sliceNumber number of slice to extract
     * @return slice of input data
     */
    public abstract InputT getSlice(InputT wholeInput, int sliceNumber);

    private HashMap<KeyT, ArrayList<ValueT>> map(InputT input) {
        ThreadPoolExecutor executor
                = (ThreadPoolExecutor) Executors.newFixedThreadPool(mapersNumber);

        List< Future<ArrayList<Pair<KeyT, ValueT>>>> futures = new ArrayList<>();
        for (int i = 0; i < mapersNumber; i++) {
            try {
                Mapper<InputT, KeyT, ValueT> mapper
                        = (Mapper<InputT, KeyT, ValueT>) maperClass.newInstance();
                mapper.setInput(getSlice(input, i));
                futures.add(executor.submit(mapper));
            } catch (Exception ex) {
            }
        }

        HashMap<KeyT, ArrayList<ValueT>> map = new HashMap<>();
        while (!futures.isEmpty()) {
            List< Future<ArrayList<Pair<KeyT, ValueT>>>> next = new ArrayList<>();
            for (Future<ArrayList<Pair<KeyT, ValueT>>> future : futures) {
                if (future.isDone()) {
                    try {
                        ArrayList<Pair<KeyT, ValueT>> res = future.get();
                        for (Pair<KeyT, ValueT> p : res) {
                            if (!map.containsKey(p.key)) {
                                map.put(p.key, new ArrayList<ValueT>());
                            }
                            map.get(p.key).add(p.value);
                        }
                    } catch (Exception ex) {
                    }
                } else {
                    next.add(future);
                }
            }
            futures = next;
        }
        executor.shutdown();

        return map;
    }

    private ArrayList<Pair<KeyT, ValueT>> reduce(HashMap<KeyT, ArrayList<ValueT>> map) {
        ThreadPoolExecutor executor
                = (ThreadPoolExecutor) Executors.newFixedThreadPool(reducersNumber);

        List< Future<Pair<KeyT, ValueT>>> futures = new ArrayList<>();
        for (KeyT key : map.keySet()) {
            try {
                Reducer<KeyT, ValueT> reducer
                        = (Reducer<KeyT, ValueT>) reducerClass.newInstance();
                reducer.setInput(map.get(key), key);
                futures.add(executor.submit(reducer));
            } catch (Exception ex) {
            }
        }

        ArrayList<Pair<KeyT, ValueT>> result = new ArrayList<>();
        while (!futures.isEmpty()) {
            List< Future<Pair<KeyT, ValueT>>> next = new ArrayList<>();
            for (Future<Pair<KeyT, ValueT>> future : futures) {
                if (future.isDone()) {
                    try {
                        result.add(future.get());
                    } catch (Exception ex) {
                    }
                } else {
                    next.add(future);
                }
            }
            futures = next;
        }
        executor.shutdown();

        return result;
    }

}
