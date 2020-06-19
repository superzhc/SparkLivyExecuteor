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
    String printSchema();

    public SparkDataFrame execute(String sql);

    SparkDataFrame execute(String sql, String alias);

    SparkDataFrame select(String... columns);

    SparkDataFrame selectExpr(String... columns);

    SparkDataFrame filter(String conditionExpr);

    SparkDataFrame where(String conditionExpr);

    SparkDataFrame drop(String... columns);

    SparkDataFrame limit(int nums);

    SparkDataFrame orderBy(String... sortCols);

    SparkDataFrame distinct();

    SparkDataFrame dropDuplicates(String... columns);

    long count();

    String show();

    String show(int numRows);

    Row[] collect();

    List<Row> collectAsList();

    Row[] take(int n);

    List<Row> takeAsList(int n);

    Row first();

    /**
     * 获取指定字段的统计信息
     * @param columns
     * @return
     */
    SparkDataFrame describe(String... columns);

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
