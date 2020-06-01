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

    public String hive(String sql);

    public String textFile(String path, final String split, Map<String, String> fields);
}
