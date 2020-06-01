package com.github.superzhc.dataframe;

import com.github.superzhc.AbstractSparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 2020年06月01日 superz add
 */
public class SparkDataFrameImpl extends AbstractSparkSession implements SparkDataFrame
{
    // private Dataset<Row> dataFrame;
    private String dfKey;
    /* 设置DataFrame的表名 */
    private String tableName;

    public SparkDataFrameImpl(String dfKey, String tableName) {
        this.dfKey = dfKey;
        this.tableName = tableName;
    }

    /**
     * 从老的DataFrameWrapper创建新的DataFrameWrapper，表名延用
     * @param dfKey
     * @return
     */
    private SparkDataFrame create(String dfKey) {
        return new SparkDataFrameImpl(dfKey, tableName);
    }

    /**
     * 以树的形式打印数据的结构信息
     */
    public void printSchema() {
        SparkDataFrameMapping.getInstance().get(dfKey).printSchema();
    }

    public SparkDataFrame execute(String sql) {
        Dataset<Row> dataFrame = SparkDataFrameMapping.getInstance().get(dfKey);
        try {
            dataFrame.createOrReplaceTempView(tableName);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("执行SQL失败");
        }
        Dataset<Row> df = spark.sql(sql);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey);
    }

    public SparkDataFrame select(String... columns) {
        if (null == columns || columns.length == 0)
            return null;

        Dataset<Row> df = SparkDataFrameMapping.getInstance().get(dfKey);
        df = df.select(columns[0], Arrays.copyOfRange(columns, 1, columns.length - 1));
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey);
    }

    /**
     * 数据的条数
     * @return
     */
    public long count() {
        return SparkDataFrameMapping.getInstance().get(dfKey).count();
    }

    public void show() {
        show(20);// 默认显示20条数据
    }

    /**
     * 预览打印部分数据
     * @param numRows
     */
    public void show(Integer numRows) {
        SparkDataFrameMapping.getInstance().get(dfKey).show(numRows);
    }

    /**
     * 获取全部数据，数据量大的时候一次性获取会出现内存溢出，慎重使用
     * 推荐使用 take 来获取部分数据
     * @return
     */
    public Row[] collect() {
        return (Row[]) SparkDataFrameMapping.getInstance().get(dfKey).collect();
    }

    /**
     * 获取全部数据，数据量大的时候一次性获取会出现内存溢出，慎重使用
     * 推荐使用 take 来获取部分数据
     * @return
     */
    public List<Row> collectAsList() {
        // 直接使用此方法，返回数据在Livy序列化调用java.util.Arrays$ArrayList.size报空指针，不使用Arrays.asList来进行操作了
        // return SparkDataFrameMapping.getInstance().get(dfKey).collectAsList();
        Row[] rows = collect();
        List<Row> lst = new ArrayList<>(rows.length);
        for (Row row : rows) {
            lst.add(row);
        }
        return lst;
    }

    /**
     * 获取前 n 行数据
     * @param n
     * @return
     */
    public Row[] take(Integer n) {
        return (Row[]) SparkDataFrameMapping.getInstance().get(dfKey).take(n);
    }

    /**
     * 获取前 n 行数据
     * @param n
     * @return
     */
    public List<Row> takeAsList(Integer n) {
        // 直接使用此方法，返回数据在Livy序列化调用java.util.Arrays$ArrayList.size报空指针，不使用Arrays.asList来进行操作了
        // return SparkDataFrameMapping.getInstance().get(dfKey).takeAsList(n);
        Row[] rows = take(n);
        List<Row> lst = new ArrayList<>(rows.length);
        for (Row row : rows) {
            lst.add(row);
        }
        return lst;
    }

    /**
     * 获取第一条数据
     * @return
     */
    public Row first() {
        return SparkDataFrameMapping.getInstance().get(dfKey).first();
    }

    /**
     * 保存数据
     * @param url
     * @param tableName
     * @param saveMode save modes are 'overwrite', 'append', 'ignore', 'error'
     * @param props
     */
    public void writeJdbc(String url, String tableName, String saveMode, Properties props) {
        Dataset<Row> df = SparkDataFrameMapping.getInstance().get(dfKey);
        df.write().mode(saveMode).jdbc(url, tableName, props);
    }

    public String getDfKey() {
        return dfKey;
    }
}
