package com.github.superzhc.dataframe;

import com.github.superzhc.livy.SparkLivyLocal;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Properties;

/**
 * 2020年06月01日 superz add
 */
public interface SparkDataFrame
{
    @Deprecated
    public void printSchema();

    public SparkDataFrame execute(String sql);

    public long count();

    /**
     * 数据在Spark服务器上打印，若需要获取数据使用take方法来获取数据
     */
    public void show();

    /**
     * 数据在Spark服务器上打印，若需要获取数据使用take方法来获取数据
     */
    public void show(int numRows);

    public Row[] collect();

    public List<Row> collectAsList();

    public Row[] take(int n);

    public List<Row> takeAsList(int n);

    public Row first();

    void saveJdbc(String url, String tableName, Properties props);

    void saveJdbc(String url, String tableName, String saveMode, Properties props);

    void saveHive(String tableName);

    void saveHive(String tableName,String saveMode);

    void saveParquet(String path);

    void saveCSV(String path);

    void saveCSV(boolean header, String path);

    void saveJson(String path);

    @SparkLivyLocal
    String key();
}
