package com.github.superzhc.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 2020年06月01日 superz add
 */
public class SparkDataFrameMapping
{
    private static SparkDataFrameMapping instance = new SparkDataFrameMapping();

    private Map<String, Dataset<Row>> map;

    private SparkDataFrameMapping() {
        map = new ConcurrentHashMap<>();
    }

    public static SparkDataFrameMapping getInstance() {
        return instance;
    }

    public Dataset<Row> get(String key) {
        return map.get(key);
    }

    /**
     * 自定义DataFrame的唯一标识
     * @param key
     * @param df
     * @return
     */
    public String put(String key, Dataset<Row> df) {
        map.put(key, df);
        return key;
    }

    /**
     * 返回对应的唯一标识key
     * @param df
     * @return
     */
    public String set(Dataset<Row> df) {
        String uuid = UUID.randomUUID().toString();
        return put(uuid, df);
    }

    public boolean contain(String key) {
        return map.containsKey(key);
    }
}
