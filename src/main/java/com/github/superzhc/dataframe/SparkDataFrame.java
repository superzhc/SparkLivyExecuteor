package com.github.superzhc.dataframe;

import com.github.superzhc.livy.SparkLivyLocal;
import org.apache.spark.sql.Row;

import java.util.List;

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
    public void show(Integer numRows);

    public Row[] collect();

    public List<Row> collectAsList();

    public Row[] take(Integer n);

    public List<Row> takeAsList(Integer n);

    public Row first();

    @SparkLivyLocal
    String key();
}
