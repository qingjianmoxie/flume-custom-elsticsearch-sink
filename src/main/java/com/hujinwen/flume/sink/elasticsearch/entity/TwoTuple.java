package com.hujinwen.flume.sink.elasticsearch.entity;

/**
 * Created by joe on 2020/4/7
 */
public class TwoTuple<T, E> {
    public final T first;
    public final E second;

    public TwoTuple(T first, E second) {
        this.first = first;
        this.second = second;
    }
}
