package com.github.wslf.simplemapreduce;

/**
 *
 * @author Wsl_F
 */
public class Pair<KeyT, ValueT> {

    public KeyT key;
    public ValueT value;

    public Pair(KeyT key, ValueT value) {
        this.key = key;
        this.value = value;
    }

}
