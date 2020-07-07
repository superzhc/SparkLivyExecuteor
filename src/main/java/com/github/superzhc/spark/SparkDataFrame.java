package com.github.superzhc.spark;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;

import com.github.superzhc.dataframe.SparkDataFrameMapping;
import com.github.superzhc.livy.SparkLivyLocal;
import com.github.superzhc.livy.SparkLivyResultProxy;
import com.github.superzhc.utils.JacksonUtils;
import com.github.superzhc.utils.LivySerUtils;

import scala.collection.JavaConversions;

/**
 * 2020年06月19日 superz add
 */
@SparkLivyResultProxy
public class SparkDataFrame extends AbstractSparkSession implements Serializable
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
     * 获取分区数
     * @return
     */
    public int getNumPartitions(){
        return dataFrame().rdd().getNumPartitions();
    }

    /**
     * 重分区，直接设置分区数量
     */
    public SparkDataFrame repartition(int numPartitions) {
        Dataset<Row> df = dataFrame().repartition(numPartitions);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 重分区，通过字段进行分区
     * @param column
     * @return
     */
    public SparkDataFrame repartition(String column) {
        Dataset<Row> df = dataFrame();
        df = df.repartition(df.col(column));
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
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
     * 分组
     * @param groupColumns  分组列
     * @param exprs         聚合列
     *                      {
     *                          "x1":"count",
     *                          "x2":"sum"    
     *                      }
     * @return
     */
    public SparkDataFrame groupBy(String[] groupColumns, Map<String, String> exprs) {
        Dataset<Row> df = dataFrame();

        RelationalGroupedDataset relationalGroupedDataset;
        if (groupColumns.length == 1) {
            relationalGroupedDataset = df.groupBy(groupColumns[0]);
        }
        else {
            String[] remainGroupColumns = Arrays.copyOfRange(groupColumns, 1, groupColumns.length - 1);
            relationalGroupedDataset = df.groupBy(groupColumns[0], remainGroupColumns);
        }
        df = relationalGroupedDataset.agg(exprs);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

//    public SparkDataFrame groupBy(String[] groupColumns, Map<String,String[]> aggExprs) {
//        Dataset<Row> df = dataFrame();
//
//        RelationalGroupedDataset relationalGroupedDataset;
//        if (groupColumns.length == 1) {
//            relationalGroupedDataset = df.groupBy(groupColumns[0]);
//        }
//        else {
//            String[] remainGroupColumns = Arrays.copyOfRange(groupColumns, 1, groupColumns.length - 1);
//            relationalGroupedDataset = df.groupBy(groupColumns[0], remainGroupColumns);
//        }
//
//        List<Column> lst = new LinkedList<>();
//        if (aggExprs.containsKey("count")) {
//            String[] count = aggExprs.get("count");
//            for (int i = 0, len = count.length; i < len; i++) {
//                lst.add(org.apache.spark.sql.functions.count(count[i]));
//            }
//        }
//
//        if(aggExprs.containsKey("max")) {
//            String[] max=aggExprs.get("max");
//            for (int i = 0, len = max.length; i < len; i++) {
//                lst.add(org.apache.spark.sql.functions.max(max[i]));
//            }
//        }
//
//        if(aggExprs.containsKey("min")) {
//            String[] min=aggExprs.get("min");
//            for (int i = 0, len = min.length; i < len; i++) {
//                lst.add(org.apache.spark.sql.functions.min(min[i]));
//            }
//        }
//
//        if(aggExprs.containsKey("avg")){
//            String[] avg=aggExprs.get("avg");
//            for (int i = 0, len = avg.length; i < len; i++) {
//                lst.add(org.apache.spark.sql.functions.avg(avg[i]));
//            }
//        }
//
//        if(lst.size()==1){
//            df=relationalGroupedDataset.agg(lst.get(0));
//        }else{
//            Column first=lst.get(0);
//            lst.remove(0);
//            Column[] remainColumns=new Column[lst.size()];
//            df=relationalGroupedDataset.agg(first,lst.toArray(remainColumns));
//        }
//
//        String dfKey = SparkDataFrameMapping.getInstance().set(df);
//        return create(dfKey, tableName);
//    }

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
        List<Row> lst = LivySerUtils.array2list(rows);
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
        // List<Row> lst = new ArrayList<>(rows.length);
        // for (Row row : rows) {
        // lst.add(row);
        // }
        List<Row> lst=LivySerUtils.array2list(rows);
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

    // region===========================持久化============================
    public SparkDataFrame cache() {
        Dataset<Row> df = dataFrame();
        df.cache();
        return this;
    }

    public SparkDataFrame persist(String storageLevel) {
        Dataset<Row> df = dataFrame();
        if (null == storageLevel || "".equals(storageLevel)) {
            df.persist();
        }
        else {
            df.persist(StorageLevel.fromString(storageLevel));
        }
        return this;
    }

    public SparkDataFrame unpersist() {
        Dataset<Row> df = dataFrame();
        df.unpersist();
        return this;
    }

    public SparkDataFrame unpersist(boolean blocking) {
        Dataset<Row> df = dataFrame();
        df.unpersist(blocking);
        return this;
    }
    // endregion========================持久化============================

    // region============================数据存储===========================

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
//        if (!url.contains("rewriteBatchedStatements")) {// MySQL服务是否开启批次写入
//            url += url.contains("?") ? "&" : "?";
//            url += "rewriteBatchedStatements=true";
//        }
//        props.put("batchsize","10000");// 批次写入MySQL 的条数
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

    // endregion==========================数据存储===========================

    @SparkLivyLocal
    public String key() {
        return dfKey;
    }

    // region =========================自定义函数==============================
    /**
     * 列的类型转换，支持的类型有: `string`, `boolean`, `byte`, `short`, `int`, `long`, `float`, `double`, `decimal`, `date`, `timestamp`.
     * @param column
     * @param to
     * @return
     */
//    public SparkDataFrame cast(String column, String to) {
//        Dataset<Row> df = dataFrame();
//        df = df.withColumn(column, df.col(column).cast(to));
//
//        SparkDataFrameMapping.getInstance().put(dfKey, df);// 类型转化，直接覆盖掉原来的就好了，不重新创建一个df
//        return this;
//    }

    /**
     * 代码项
     * @param column
     * @param newColumn
     * @param items
     * @return
     */
    public SparkDataFrame codeitem(String column, String newColumn, String items) {
        Dataset<Row> df = dataFrame();

        // 2020年7月1日 将代码项设置成广播变量
        Broadcast<String> broadcast=JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(items);

        // 继承UDF1~22的接口即可实现自定义函数的创建，简单的可类似如下的匿名函数的方式
        UDF1 udf1 = new UDF1<String, String>()
        {
            @Override
            public String call(String s) throws Exception {
                String value = JacksonUtils.convert(broadcast.getValue()).getString(s);
                return null == value ? "" : value;
            }
        };

        // 自定义函数的使用
        String fun = "codeitem";
        // spark.udf().register(fun, new CodeItem(items), DataTypes.StringType);
        spark.udf().register(fun, udf1, DataTypes.StringType);
        // df = df.selectExpr("*", "codeitem(" + column + ") as " + newColumn);
        df = df.withColumn(newColumn, org.apache.spark.sql.functions.callUDF(fun, df.col(column)));

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 截取子字符串
     * @param column
     * @param newColumn
     * @param start
     * @param end
     * @return
     */
    public SparkDataFrame substring(String column, String newColumn, int start, int end) {
        Dataset<Row> df = dataFrame();

        UDF1<String, String> udf1 = new UDF1<String, String>()
        {
            @Override
            public String call(String s) throws Exception {
                if (null == s || "".equals(s))
                    return s;

                int len = s.length();
                if (len < start) {
                    return null;
                }
                else {
                    int e = len > end ? end : len; // 防止越界
                    return s.substring(start, e);
                }
            }
        };

        String fun = "substring";
        spark.udf().register(fun, udf1, DataTypes.StringType);
        df = df.withColumn(newColumn, org.apache.spark.sql.functions.callUDF(fun, df.col(column)));

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 时间列格式化成指定格式的字符串
     * @param column
     * @param newColumn
     * @param pattern
     * @return
     */
    public SparkDataFrame dateformat(String column, String newColumn, String pattern) {
        Dataset<Row> df = dataFrame();
        df = df.withColumn(newColumn, org.apache.spark.sql.functions.date_format(df.col(column), pattern));

//        Broadcast<String> broadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(pattern);
//        UDF1<Date, String> udf1 = new UDF1<Date, String>()
//        {
//            @Override
//            public String call(Date date) throws Exception {
//                if(null==date)
//                    return null;
//                return new SimpleDateFormat(broadcast.getValue()).format(date);
//            }
//        };
//
//        String fun = "dateformat";
//        spark.udf().register(fun, udf1, DataTypes.StringType);
//        df = df.withColumn(newColumn, org.apache.spark.sql.functions.callUDF(fun, df.col(column)));

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 将字符串时间列格式化成指定格式的字符串
     * @param column
     * @param newColumn
     * @param fromPattern
     * @param toPattern
     * @return
     */
    public SparkDataFrame strdateformat(String column, String newColumn, String fromPattern, String toPattern) {
        Dataset<Row> df = dataFrame();
        df = df.withColumn(newColumn,
                org.apache.spark.sql.functions.date_format(
                        org.apache.spark.sql.functions.unix_timestamp(df.col(column), fromPattern).cast("timestamp"),
                        toPattern));

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }
    // endregion =========================自定义函数==============================
}
