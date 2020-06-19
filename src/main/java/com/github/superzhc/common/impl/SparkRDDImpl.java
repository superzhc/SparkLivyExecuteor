package com.github.superzhc.common.impl;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.github.superzhc.common.SparkRDD;
import com.github.superzhc.livy.AbstractSparkSession;
import com.github.superzhc.rdd.SparkRDDMapping;

/**
 * 2020年06月09日 superz add
 */
public class SparkRDDImpl extends AbstractSparkSession implements SparkRDD
{
    private JavaSparkContext sc;

    private JavaSparkContext sparkContext() {
        if (null == sc) {
            /* 将Scala的SparkContext转换成Java版本的JavaSparkContext */
            sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        }
        return sc;
    }

    public <T> String parallelize(List<T> list) {
        // JavaRDD<T> rdd = sparkContext().parallelize(list);
        // return SparkRDDMapping.getInstance().set(rdd);
        return parallelize(list, sparkContext().defaultParallelism());
    }

    public <T> String parallelize(List<T> list, int numSlices) {
        JavaRDD<T> rdd = sparkContext().parallelize(list, numSlices);
        return SparkRDDMapping.getInstance().set(rdd);
    }

    public String textFile(String path) {
        return textFile(path, sparkContext().defaultParallelism());
    }

    /**
     * @param path
     *      1. 具体的文件路径地址：C:\test\spark.txt
     *      2. 支持模式匹配：C:\test\*.txt
     *      3. 支持文件目录：C:\test
     *      4. 支持多个路径，可以使用逗号隔开：C:\test,C:\test1
     * @param partitions
     * @return
     */
    public String textFile(String path, int partitions) {
        JavaRDD<String> rdd = sparkContext().textFile(path, partitions);
        return SparkRDDMapping.getInstance().set(rdd);
    }
}
