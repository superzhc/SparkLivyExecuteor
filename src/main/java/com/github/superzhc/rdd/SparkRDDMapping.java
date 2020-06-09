package com.github.superzhc.rdd;

import org.apache.spark.api.java.JavaRDD;

import com.github.superzhc.common.SparkObjectMapping;

/**
 * 2020年06月09日 superz add
 */
public class SparkRDDMapping<T> extends SparkObjectMapping<JavaRDD<T>>
{
    private static SparkRDDMapping instance=new SparkRDDMapping();

    private SparkRDDMapping(){
        super();
    }

    public static SparkRDDMapping getInstance(){
        return instance;
    }
}
