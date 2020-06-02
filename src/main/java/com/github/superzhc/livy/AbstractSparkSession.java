package com.github.superzhc.livy;

import org.apache.spark.sql.SparkSession;

/**
 * 2020年06月01日 superz add
 */
public class AbstractSparkSession
{
    protected SparkSession spark;

    public SparkSession getSpark() {
        return spark;
    }

    public void setSpark(SparkSession spark) {
        this.spark = spark;
    }
}
