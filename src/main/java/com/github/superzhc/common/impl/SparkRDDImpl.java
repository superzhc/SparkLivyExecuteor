package com.github.superzhc.common.impl;

import com.github.superzhc.common.SparkRDD;
import com.github.superzhc.livy.AbstractSparkSession;
import com.github.superzhc.rdd.SparkRDDMapping;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 2020年06月09日 superz add
 */
public class SparkRDDImpl extends AbstractSparkSession implements SparkRDD
{
    private JavaSparkContext sparkContext() {
        /* 将Scala的SparkContext转换成Java版本的JavaSparkContext */
        return JavaSparkContext.fromSparkContext(spark.sparkContext());
    }

    public <T> String parallelize(List<T> list) {
        JavaRDD<T> rdd = sparkContext().parallelize(list);
        return SparkRDDMapping.getInstance().set(rdd);
    }

    public <T> String parallelize(T... arr) {
        JavaRDD<T> rdd = sparkContext().parallelize(Arrays.asList(arr));
        return SparkRDDMapping.getInstance().set(rdd);
    }
}
