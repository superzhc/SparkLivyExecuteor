package com.github.superzhc.dataframe;

import com.github.superzhc.common.SparkObjectMapping;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 2020年06月01日 superz add
 */
public class SparkDataFrameMapping extends SparkObjectMapping<Dataset<Row>>
{
    private static SparkDataFrameMapping instance = new SparkDataFrameMapping();

    private SparkDataFrameMapping() {
        super();
    }

    public static SparkDataFrameMapping getInstance() {
        return instance;
    }
}
