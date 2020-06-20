package com.github.superzhc.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.github.superzhc.dataframe.SparkDataFrameMapping;
import com.github.superzhc.livy.SparkLivyLocal;

import scala.collection.JavaConversions;

/**
 * 2020年06月19日 superz add
 */
public class SparkDataFrame extends AbstractSparkSession
{
    // private Dataset<Row> dataFrame;
    private String dfKey;
    /* 设置DataFrame的表名 */
    private String tableName;

    public SparkDataFrame(){}

    public SparkDataFrame(String dfKey, String tableName) {
        this.dfKey = dfKey;
        this.tableName = tableName;
    }

    private Dataset<Row> dataFrame(){
        return SparkDataFrameMapping.getInstance().get(dfKey);
    }

    /**
     * 从老的DataFrameWrapper创建新的DataFrameWrapper，表名延用
     * @param dfKey
     * @return
     */
    private SparkDataFrame create(String dfKey,String tableName) {
        return new SparkDataFrame(dfKey, tableName);
    }

    /**
     * 以树的形式打印数据的结构信息
     */
    public String printSchema() {
        // 查询源码的实现，服务器打印一份，本地返回一份
        String s = dataFrame().schema().treeString();
        System.out.println(s);
        return tableName + s.substring(4);// 将DataFrame返回的根是root替换成SparkDataFrame的表名
    }

    public SparkDataFrame execute(String sql) {
        return execute(sql, tableName);
    }

    /**
     * 执行的语句，并设置别名
     * @param sql
     * @param alias
     * @return
     */
    public SparkDataFrame execute(String sql, String alias) {
        Dataset<Row> dataFrame = dataFrame();
        try {
            dataFrame.createOrReplaceTempView(tableName);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("执行SQL失败");
        }
        Dataset<Row> df = spark.sql(sql);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, alias);
    }

    public SparkDataFrame select(String... columns) {
        if (null == columns || columns.length == 0)
            return null;

        Dataset<Row> df = dataFrame();
        // fix:读取的列只有一列的情况，选择正确的方法
        if (columns.length == 1)
            df = df.select(columns[0]);
        else
            df = df.select(columns[0], Arrays.copyOfRange(columns, 1, columns.length - 1));

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 对指定字段进行特殊处理
     * @param columns
     * @return
     */
    public SparkDataFrame selectExpr(String... columns) {
        if (null == columns || columns.length == 0)
            return null;

        Dataset<Row> df = dataFrame();
        df = df.selectExpr(columns);

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    public SparkDataFrame rename(String oldColumnName, String columnName) {
        Dataset<Row> df = dataFrame();
        df = df.withColumnRenamed(oldColumnName, columnName);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    public SparkDataFrame filter(String conditionExpr) {
        Dataset<Row> df = dataFrame();
        df = df.filter(conditionExpr);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    public SparkDataFrame where(String conditionExpr) {
        return filter(conditionExpr);
    }

    /**
     * 去除指定字段，保留其他字段
     * @param columns
     * @return
     */
    public SparkDataFrame drop(String... columns) {
        Dataset<Row> df = dataFrame();
        df = df.drop(columns);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 获取指定DataFrame的前n行记录，得到一个新的DataFrame对象
     * @param nums
     * @return
     */
    public SparkDataFrame limit(int nums) {
        Dataset<Row> df = dataFrame();
        df = df.limit(nums);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 按指定字段排序，默认为升序
     * @param sortCols
     * @return
     */
    public SparkDataFrame orderBy(String... sortCols) {
        if (null == sortCols || sortCols.length == 0)
            return null;

        Dataset<Row> df = dataFrame();
        if (sortCols.length == 1)
            df = df.orderBy(sortCols[0]);
        else
            df = df.select(sortCols[0], Arrays.copyOfRange(sortCols, 1, sortCols.length - 1));
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 返回当前DataFrame中不重复的Row记录
     * @return
     */
    public SparkDataFrame distinct() {
        Dataset<Row> df = dataFrame();
        df = df.distinct();
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 根据指定字段去重
     * @param columns
     * @return
     */
    public SparkDataFrame dropDuplicates(String... columns) {
        Dataset<Row> df = dataFrame();
        df = df.dropDuplicates(columns);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    public SparkDataFrame union(String key) {
        return union(key, tableName);
    }

    /**
     * 对两个DataFrame进行组合
     * @param key
     * @param alias
     * @return
     */
    public SparkDataFrame union(String key, String alias) {
        Dataset<Row> df = dataFrame();
        Dataset<Row> otherDf = SparkDataFrameMapping.getInstance().get(key);
        df = df.union(otherDf);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, alias);
    }

    public SparkDataFrame unionAll(String key) {
        return unionAll(key, tableName);
    }

    /**
     * 对两个DataFrame进行组合
     * @param key
     * @param alias
     * @return
     */
    public SparkDataFrame unionAll(String key, String alias) {
        Dataset<Row> df = dataFrame();
        Dataset<Row> otherDf = SparkDataFrameMapping.getInstance().get(key);
        df = df.unionAll(otherDf);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, alias);
    }

    public SparkDataFrame join(String key, String... columns) {
        return join(key, columns, "inner", tableName);
    }

    public SparkDataFrame join(String key, String[] columns, String joinType) {
        return join(key, columns, joinType, tableName);
    }

    /**
     * Join操作
     * @param key
     * @param alias
     * @param joinType
     * @param columns
     * @return
     */
    public SparkDataFrame join(String key, String[] columns, String joinType, String alias) {
        Dataset<Row> df = dataFrame();
        Dataset<Row> otherDf = SparkDataFrameMapping.getInstance().get(key);
        if (null == columns || columns.length == 0)
            df = df.join(otherDf);// 笛卡尔积
        else if (columns.length == 1)
            df = df.join(otherDf, columns[0]);
        else
            df = df.join(otherDf, JavaConversions.asScalaIterator(Arrays.asList(columns).iterator()).toSeq(), joinType);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, alias);
    }

    public SparkDataFrame intersect(String key) {
        return intersect(key, tableName);
    }

    /**
     * 获取两个DataFrame中共有的记录
     * @param key
     * @param alias
     * @return
     */
    public SparkDataFrame intersect(String key, String alias) {
        Dataset<Row> df = dataFrame();
        Dataset<Row> otherDf = SparkDataFrameMapping.getInstance().get(key);
        df = df.intersect(otherDf);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, alias);
    }

    public SparkDataFrame except(String key) {
        return except(key, tableName);
    }

    /**
     * 获取一个DataFrame中有另一个DataFrame中没有的记录
     * @param key
     * @param alias
     * @return
     */
    public SparkDataFrame except(String key, String alias) {
        Dataset<Row> df = dataFrame();
        Dataset<Row> otherDf = SparkDataFrameMapping.getInstance().get(key);
        df = df.except(otherDf);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, alias);
    }

    /**
     * 数据的条数
     * @return
     */
    public long count() {
        return dataFrame().count();
    }

    public String show() {
        return show(20);// 默认显示20条数据
    }

    /**
     * 预览打印部分数据
     * @param numRows
     */
    public String show(int numRows) {
        // 查询源码的实现，服务器一份数据，本地一份数据
        String s=dataFrame().showString(numRows,20);
        System.out.println(s);
        return s;
    }

    /**
     * 获取全部数据，数据量大的时候一次性获取会出现内存溢出，慎重使用
     * 推荐使用 take 来获取部分数据
     * @return
     */
    public Row[] collect() {
        return (Row[]) dataFrame().collect();
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
    public Row[] take(int n) {
        return (Row[]) dataFrame().take(n);
    }

    /**
     * 获取前 n 行数据
     * @param n
     * @return
     */
    public List<Row> takeAsList(int n) {
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
        return dataFrame().first();
    }

    /**
     * 获取指定字段的统计信息
     * @param columns
     * @return
     */
    public SparkDataFrame describe(String... columns) {
        if (null == columns || columns.length == 0)
            return null;

        Dataset<Row> df = dataFrame().describe(columns);

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    public void saveJdbc(String url, String tableName, Properties props){
        saveJdbc(url,tableName,"error",props);
    }

    /**
     * 保存数据
     * @param url
     * @param tableName
     * @param saveMode save modes are 'overwrite', 'append', 'ignore', 'error'
     *           SaveMode.ErrorIfExists(default)	“error”(default)	如果文件存在，则报错
     *           SaveMode.Append	“append”	追加
     *           SaveMode.Overwrite	“overwrite”	覆写
     *           SaveMode.Ignore	“ignore”	数据存在，则忽略
     * @param props
     */
    public void saveJdbc(String url, String tableName, String saveMode, Properties props) {
        Dataset<Row> df = dataFrame();
        df.write().mode(saveMode).jdbc(url, tableName, props);
    }

    public void saveHive(String tableName) {
        saveHive(tableName,"error");
    }

    public void saveHive(String tableName,String saveMode){
        Dataset<Row> df=dataFrame();
        df.write().mode(saveMode).saveAsTable(tableName);
    }

    public void saveParquet(String path) {
        Dataset<Row> df = dataFrame();
        df.write().save(path);
    }

    public void saveCSV(String path) {
        saveCSV(false, path);
    }

    public void saveCSV(boolean header, String path) {
        Dataset<Row> df = dataFrame();
        df.write().option("header", header).csv(path);
    }

    public void saveJson(String path) {
        Dataset<Row> df = dataFrame();
        df.write().json(path);
    }

    @SparkLivyLocal
    public String key() {
        return dfKey;
    }
}
