package com.github.wslf.simplemapreduce;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;

/**
 *
 * @author Wsl_F
 */
public abstract class Reducer<KeyT, ValueT> implements Callable<Pair<KeyT, ValueT>> {

    public ArrayList<ValueT> input = null;
    public KeyT key = null;

    public Reducer() {
        this.input = null;
        this.key = null;
    }

    public void setInput(Collection<ValueT> value, KeyT key) {
        this.input = new ArrayList<ValueT> (value);
        this.key = key;
    }

    public abstract Pair<KeyT, ValueT> reduce();

    public Pair<KeyT, ValueT> call() {
        return input == null ? null : reduce();
    }
}
