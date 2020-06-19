package com.github.superzhc.spark;

import org.apache.spark.sql.SparkSession;

/**
 * 2020年06月01日 superz add
 */
public class AbstractSparkSession
{
    /* 2020年6月8日 该字段不进行序列化 */
    transient protected SparkSession spark;

    public SparkSession getSpark() {
        return spark;
    }

    public void setSpark(SparkSession spark) {
        this.spark = spark;
    }
}
