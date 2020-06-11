package com.github.superzhc.common;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 2020年06月01日 superz add
 */
public interface SparkSQL
{
    public String jdbc(String url, String sql);

    public String jdbc(String url, String sql, Properties props);

    public String jdbc(String url, String username, String password, String sql);

    public String jdbc(String url, String username, String password, String sql, Properties props);

    String jdbc(String url, String sql, String[] predicates, Properties props);

    String jdbc4Partition(String url, String sql, String partitionColumn, Long lowerBound, Long upperBound,
            Integer numPartitions);

    String jdbc4Partition(String url, String sql, String partitionColumn, Long lowerBound, Long upperBound,
            Integer numPartitions, Properties props);

    public String hive(String sql);

    String json(String... paths);

    public String json(List<String> lst);

    String parquet(String... paths);

    String csv(String... paths);

    String csv(boolean header, String... paths);
}
