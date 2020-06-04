package com.github.superzhc;

import java.util.Map;
import java.util.Properties;

/**
 * 2020年06月01日 superz add
 */
public interface SparkOperate
{
    public String jdbc(String url, String sql);

    public String jdbc(String url, String sql, Properties props);

    public String jdbc(String url, String username, String password, String sql);

    public String jdbc(String url, String username, String password, String sql, Properties props);

    String jdbc(String url, String sql, String[] predicates, Properties props);

    public String hive(String sql);

    String json(String... paths);

    String parquet(String... paths);

    String csv(String... paths);

    String csv(boolean header, String... paths);
}
