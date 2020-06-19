package com.github.superzhc.common;

import java.util.List;

/**
 * 2020年06月09日 superz add
 */
public interface SparkRDD
{
    <T> String parallelize(List<T> list);

    <T> String parallelize(List<T> list, int numSlices);

    String textFile(String path);

    String textFile(String path, int partitions);
}
