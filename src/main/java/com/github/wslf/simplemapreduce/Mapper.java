package com.github.wslf.simplemapreduce;

import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 *
 * @author Wsl_F
 */
public abstract class Mapper<InputT, KeyT, ValueT> implements Callable<ArrayList<Pair<KeyT, ValueT>>> {

    protected InputT input;

    public abstract ArrayList<Pair<KeyT, ValueT>> map();

    public Mapper() {
        this.input = null;
    }

    public void setInput(InputT input) {
        this.input = input;
    }

    @Override
    public ArrayList<Pair<KeyT, ValueT>> call() {
        return input == null ? null : map();
    }

}
