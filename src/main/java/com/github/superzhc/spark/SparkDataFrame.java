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
     * 示例：
     * |summary|  age|
     * +-------+-----+
     * |  count|   30| 记录条数
     * |   mean|19.73| 平均值
     * | stddev|0.907| 样本标准差
     * |    min|   18| 最小值
     * |    max|   22| 最大值
     * +-------+-----+
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

    // region===========================聚合============================

    /**
     * 获取列的最大值
     * @param column
     * @return
     */
    public Object max(String column) {
        Dataset<Row> df = dataFrame();
        df = df.select(org.apache.spark.sql.functions.max(column));
        return df.head().get(0);
    }

    /**
     * 获取列的最小值
     * @param column
     * @return
     */
    public Object min(String column) {
        Dataset<Row> df = dataFrame();
        df = df.select(org.apache.spark.sql.functions.min(column));
        return df.head().get(0);
    }

    /**
     * 获取列的平均值
     * @param column
     * @return
     */
    public Object avg(String column) {
        Dataset<Row> df = dataFrame();
        df = df.select(org.apache.spark.sql.functions.avg(column));
        return df.head().get(0);
    }

    /**
     * 获取列的总和
     * @param column
     * @return
     */
    public Object sum(String column) {
        Dataset<Row> df = dataFrame();
        df = df.select(org.apache.spark.sql.functions.sum(column));
        return df.head().get(0);
    }

    /**
     * 获取列不重复值的个数
     * @param column
     * @return
     */
    public Object countDistinct(String column) {
        Dataset<Row> df = dataFrame();
        df = df.select(org.apache.spark.sql.functions.countDistinct(column));
        return df.head().get(0);
    }

    /**
     * 聚合操作
     * @param aggExprs 示例：
     *                 {
     *                  "age":"mean",
     *                  "phone":"max" 
     *                 }
     * @return
     */
    public SparkDataFrame agg(Map<String, String> aggExprs) {
        Dataset<Row> df = dataFrame();
        df = df.agg(aggExprs);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }
    // endregion========================聚合============================

    // region===========================数据采样============================
    public SparkDataFrame sample(Boolean withReplacement, Double fraction) {
        Dataset<Row> df = dataFrame();
        df = df.sample(withReplacement, fraction);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 随机采样
     * @param withReplacement
     * @param fraction 采样的比列
     * @param seed 随机数种子
     * @return
     */
    public SparkDataFrame sample(Boolean withReplacement, Double fraction, Long seed) {
        Dataset<Row> df = dataFrame();
        df = df.sample(withReplacement, fraction, seed);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    public List<Row> takeSample(Boolean withReplacement, int num) {
        Dataset<Row> df = dataFrame();
        List<Row> lst = df.toJavaRDD().takeSample(withReplacement, num);
        return LivySerUtils.list(lst);
    }

    public List<Row> takeSample(Boolean withReplacement, Integer num, Long seed) {
        Dataset<Row> df = dataFrame();
        List<Row> lst = df.toJavaRDD().takeSample(withReplacement, num, seed);
        return LivySerUtils.list(lst);
    }
    // endregion========================数据采样============================

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
    public SparkDataFrame cast(String column, String to) {
        Dataset<Row> df = dataFrame();
        df = df.withColumn(column, df.col(column).cast(to));

        SparkDataFrameMapping.getInstance().put(dfKey, df);// 类型转化，直接覆盖掉原来的就好了，不重新创建一个df
        return this;
    }

    /**
     * 对指定列填充统一的值
     * 注：时间类型不可用
     * @param value   统一的值
     * @param columns 填充的列
     * @return
     */
    public SparkDataFrame fillNa(String value, String... columns) {
        Dataset<Row> df = dataFrame();
        df = df.na().fill(value, columns);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 对指定的列填充空值
     * @param map 列：值
     *            {
     *              "col1":"val1",
     *              "col2":"val2",
     *              ...
     *              "colN":"valN"
     *            }
     * @return
     */
    public SparkDataFrame fillNa(Map<String, Object> map) {
        Dataset<Row> df = dataFrame();
        df = df.na().fill(map);
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

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
    public SparkDataFrame substring(String column, String newColumn, Integer start, Integer end) {
        Dataset<Row> df = dataFrame();
        
        UDF1<String, String> udf1 = new UDF1<String, String>()
        {
            @Override
            public String call(String s) throws Exception {
                if (null == s || "".equals(s))
                    return s;

                int len = s.length();
                if (null != start && len < start) {
                    return null;
                }
                else {
                    int start1 = start == null ? 0 : start;
                    int e = (null == end || len <= end) ? len : end; // 防止越界
                    return s.substring(start1, e);
                }
            }
        };

        String fun = "substring2";
        spark.udf().register(fun, udf1, DataTypes.StringType);
        df = df.withColumn(newColumn, org.apache.spark.sql.functions.callUDF(fun, df.col(column)));

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 根据子字符串向前向后截取一定位置的字符串
     * @param column
     * @param newColumn
     * @param substring
     * @param front
     * @param back
     * @return
     */
    public SparkDataFrame substring(String column, String newColumn, String substring, Integer front, Integer back) {
        Dataset<Row> df = dataFrame();

        UDF1<String, String> udf1 = new UDF1<String, String>()
        {
            @Override
            public String call(String s) throws Exception {
                if (!s.contains(substring))
                    return null;

                int index = s.indexOf(substring);
                int start = (index - front) < 0 ? 0 : index - front;
                int end = (index + back) > s.length() ? s.length() : index + back;
                return s.substring(start, end);
            }
        };

        String fun = "substring3";
        spark.udf().register(fun, udf1, DataTypes.StringType);
        df = df.withColumn(newColumn, org.apache.spark.sql.functions.callUDF(fun, df.col(column)));

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 列内容替换
     * @param column
     * @param newColumn
     * @param oldSubstring
     * @param newSubstring
     * @return
     */
    public SparkDataFrame replace(String column,String newColumn,String oldSubstring,String newSubstring){
        Dataset<Row> df = dataFrame();

        UDF1<String, String> udf1 = new UDF1<String, String>()
        {
            @Override
            public String call(String s) throws Exception {
                if(null==s) return null;

                return s.replaceAll(oldSubstring,newSubstring);
            }
        };

        String fun = "replace";
        spark.udf().register(fun, udf1, DataTypes.StringType);
        df = df.withColumn(newColumn, org.apache.spark.sql.functions.callUDF(fun, df.col(column)));

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 左填充
     * @param column
     * @param newColumn
     * @param prefix
     * @return
     */
    public SparkDataFrame lpad(String column,String newColumn,String prefix){
        Dataset<Row> df = dataFrame();

        UDF1<String, String> udf1 = new UDF1<String, String>()
        {
            @Override
            public String call(String s) throws Exception {
                if(null==s) return null;// 对null不进行填充

                return prefix+s;
            }
        };

        String fun = "lpad";
        spark.udf().register(fun, udf1, DataTypes.StringType);
        df = df.withColumn(newColumn, org.apache.spark.sql.functions.callUDF(fun, df.col(column)));

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 右填充
     * @param column
     * @param newColumn
     * @param suffix
     * @return
     */
    public SparkDataFrame rpad(String column,String newColumn,String suffix){
        Dataset<Row> df = dataFrame();

        UDF1<String, String> udf1 = new UDF1<String, String>()
        {
            @Override
            public String call(String s) throws Exception {
                if(null==s) return null;// 对null不进行填充

                return s+suffix;
            }
        };

        String fun = "rpad";
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

    /**
     * 多个列进行合并成一列
     * @param columns
     * @param separator
     * @param target
     * @return
     */
    public SparkDataFrame concat(String[] columns, String separator, String target) {
        Dataset<Row> df = dataFrame();

        int colLength = columns.length;
        Column[] cols = new Column[colLength];
        for (int i = 0; i < colLength; i++)
            cols[i] = df.col(columns[i]);

        df = df.withColumn(target, org.apache.spark.sql.functions.concat_ws(separator, cols));
        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 一列分割成多列
     * @param column
     * @param separator
     * @param target
     * @param remain
     * @return
     */
    public SparkDataFrame split(String column, String separator, String[] target, boolean remain) {
        Dataset<Row> df = dataFrame();

        String splitColName = UUID.randomUUID().toString();
        df = df.withColumn(splitColName, org.apache.spark.sql.functions.split(df.col(column), separator));
        for (int i = 0, len = target.length; i < len; i++) {
            if (i == len - 1 && remain) {
                UDF1<String, String> udf1 = new UDF1<String, String>()
                {
                    @Override
                    public String call(String s) throws Exception {
                        if (null == s)
                            return null;

                        String[] ss = s.split(separator);
                        if (null != ss && ss.length >= len) {
                            StringBuilder sb = new StringBuilder();
                            for (int j = len - 1; j < ss.length; j++) {
                                sb.append(separator).append(ss[j]);
                            }
                            return sb.substring(1);
                        }
                        return null;
                    }
                };
                String fun = "splitColumn";
                spark.udf().register(fun, udf1, DataTypes.StringType);
                df = df.withColumn(target[i], org.apache.spark.sql.functions.callUDF(fun, df.col(column)));
            }
            else {
                df = df.withColumn(target[i], df.col(splitColName).getItem(i));
            }
        }

        df = df.drop(splitColName);// 删除内容进行分割的列

        String dfKey = SparkDataFrameMapping.getInstance().set(df);
        return create(dfKey, tableName);
    }

    /**
     * 列的维度下数据的占比
     * @param column
     * @param otherColumns
     * @return
     */
    public String columnsValueRatio(String column,String... otherColumns){
        Dataset<Row> df = dataFrame();
        // 获取数据总的条数
        long count = df.count();

        Dataset<Row> df2 = df.groupBy(column, otherColumns).count();
        String[] columns = df2.columns();
        Row[] rows = (Row[]) df2.collect();

        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Row row : rows) {
            sb.append("{");
            StringBuilder sb2 = new StringBuilder("");
            for (String item : columns) {
                sb2.append(",");
                if ("count".equals(item)) {
                    sb2.append("\"ration\":" + ((Long) row.getAs(item)) * 0.1 / count);
                }
                else {
                    sb2.append("\"" + item + "\":\"" + row.getAs(item) + "\"");
                }
            }
            sb.append(sb2.length() > 0 ? sb2.substring(1) : "");
            sb.append("},");
        }
        String result = sb.substring(0, sb.length() - 1) + "]";
        return result;
    }

    /**
     * 计算指定条件下数据的占比
     * @param condition
     * @return
     */
    public double percentage(String condition) {
        Dataset<Row> df = dataFrame();
        // 获取总数
        long count = df.count();

        // 获取某个条件下数据的总数
        long partCount = df.where(condition).count();

        return partCount * 1.0 / count;
    }

    /**
     * 数值层级
     * @param column
     * @param level
     * @return
     */
    public String NumberLevel(String column, Integer level) {
        Dataset<Row> df = dataFrame();
        // 获取的总数
        long count = df.count();

        // 对数值列进行排序
        df = df.orderBy(column);

        long segment = count / level;
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        Double percent = (1.0 / level);
        for (int i = 0; i <= level; i++) {
            Double proportion = percent * i;
            Object value = df.limit((int) Math.round(proportion * count))
                    .select(org.apache.spark.sql.functions.last(column)).first().get(0);
            sb.append("\"" + (Math.round(proportion * 100)) + "%\"").append(":").append("\"" + value + "\"")
                    .append(",");
        }
        String result = sb.substring(0, sb.length() - 1) + "}";
        return result;
    }
    // endregion =========================自定义函数==============================
}
