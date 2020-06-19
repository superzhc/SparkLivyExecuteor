package com.github.superzhc.common.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.github.superzhc.common.SparkSQL;
import com.github.superzhc.utils.Driver;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.github.superzhc.dataframe.SparkDataFrameMapping;
import com.github.superzhc.livy.AbstractSparkSession;

/**
 * 2020年06月01日 superz add
 */
public class SparkSQLImpl extends AbstractSparkSession implements SparkSQL
{
    @Override
    public String jdbc(String url, String sql) {
        return jdbc(url, sql, new Properties());
    }

    @Override
    public String jdbc(String url, String sql, Properties props) {
        /*
         * // 2020年6月4日 用户传递配置来实现分区的示例
         * // 整数类型
         * props.put("partitionColumn", "id");// 分区列，列为整数类型和时间
         * props.put("lowerBound", "1");// 下限
         * props.put("upperBound", "123456789");// 上限
         * props.put("numPartitions", "50");// 分区数
         * // 时间类型
         * props.put("partitionColumn", "date1");// 分区列，列为整数类型和时间
         * props.put("lowerBound", "2019-01-01");// 下限
         * props.put("upperBound", "2020-06-11");// 上限
         * props.put("numPartitions", "50");// 分区数
         */
        if (!props.containsKey("driver"))
            props.put("driver", Driver.match(url).fullClassName());
        Dataset<Row> df = spark.read().jdbc(url, sql, props);
        // 修复：Dataset并不能作为返回值返回，返回一个唯一标识
        // return df;
        return SparkDataFrameMapping.getInstance().set(df);
    }

    /**
     * 根据任意字段进行分区
     * @param url
     * @param sql
     * @param predicates
     * @param props
     * @return
     */
    @Override
    public String jdbc(String url, String sql, String[] predicates, Properties props) {
        // 谓词设置
        // predicates = new String[] {"reportDate<'2020-01-01'","reportDate>='2020-01-01'" };
        if (!props.containsKey("driver"))
            props.put("driver", Driver.match(url).fullClassName());
        Dataset<Row> df = spark.read().jdbc(url, sql, predicates, props);
        return SparkDataFrameMapping.getInstance().set(df);
    }

    @Override
    public String jdbc(String url, String username, String password, String sql) {
        return jdbc(url, username, password, sql, new Properties());
    }

    @Override
    public String jdbc(String url, String username, String password, String sql, Properties props) {
        props.put("user", username);
        props.put("password", password);
        return jdbc(url, sql, props);
    }

    public String jdbc4Partition(String url, String sql, String partitionColumn, Long lowerBound, Long upperBound,
            Integer numPartitions) {
        return jdbc4Partition(url, sql, partitionColumn, lowerBound, upperBound, numPartitions, new Properties());
    }

    public String jdbc4Partition(String url, String sql, String partitionColumn, Long lowerBound, Long upperBound,
            Integer numPartitions, Properties props) {
        props.put("partitionColumn", partitionColumn);// 分区列，列为整数类型和时间
        props.put("lowerBound", lowerBound);// 下限
        props.put("upperBound", upperBound);// 上限
        props.put("numPartitions", numPartitions);// 分区数
        return jdbc(url, sql, props);
    }

    @Override
    public String hive(String sql) {
        Dataset<Row> df = spark.sql(sql);
        // 修复：Dataset并不能作为返回值返回，返回一个唯一标识
        // return df;
        return SparkDataFrameMapping.getInstance().set(df);
    }

    /**
     * 读取json文件
     * @param paths
     * @return
     */
    public String json(String... paths) {
        Dataset<Row> df = spark.read().json(paths);
        return SparkDataFrameMapping.getInstance().set(df);
    }

    /**
     * Json字符串转换成DataFrame
     * @param lst
     * @return
     */
    public String json(List<String> lst) {
        JavaRDD<String> rdd = JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(lst);
        Dataset<Row> df = spark.read().json(rdd);
        return SparkDataFrameMapping.getInstance().set(df);
    }

    public String parquet(String... paths) {
        Dataset<Row> df = spark.read().load(paths);
        return SparkDataFrameMapping.getInstance().set(df);
    }

    public String csv(String... paths) {
        return csv(false, paths);
    }

    public String csv(boolean header, String... paths) {
        Dataset<Row> df = spark.read().option("header", true).csv(paths);
        return SparkDataFrameMapping.getInstance().set(df);
    }

    public String hbase(String zookeeper, Integer port, String tableName) {
        // 原生的方式
        // Configuration config= HBaseConfiguration.create();
        // config.set("hbase.zookeeper.quorum", zookeeper);
        // config.set("hbase.zookeeper.property.clientPort", String.valueOf(port));
        // config.set("hbase.mapreduce.inputtable",tableName);
        // JavaRDD
        // rdd=spark.sparkContext().newAPIHadoopRDD(config,TableInputFormat.class,
        // ImmutableBytesWritable.class, Result.class);

        // TODO

        throw new RuntimeException("该方法尚未实现");
    }

    /**
     * 方法的实现不是很好
     * @param path
     * @param split
     * @param fields
     * @return
     */
    public String textFile(String path, final String split, Map<String, String> fields) {
        JavaRDD<String> rdd = spark.read().textFile(path).toJavaRDD();
        JavaRDD<Row> rdd1 = rdd.map(new Function<String, Row>()
        {
            @Override
            public Row call(String v1) throws Exception {
                String[] columns = v1.split(split);
                String[] columns2 = new String[fields.size()];
                if (columns != null && columns.length > 0) {
                    for (int i = 0, //
                    len = (columns.length < columns2.length ? columns.length : columns2.length); // 计算出公有部分，其余的都为空
                            i < len; i++) {
                        columns2[i] = columns[i];
                    }
                }
                return RowFactory.create(columns2);
            }
        });

        List<StructField> structFields = new ArrayList<>(fields.size());
        for (Map.Entry<String, String> field : fields.entrySet()) {
            DataType fieldDataType;
            switch (field.getValue().toLowerCase()) {
                case "int":
                case "integer":
                    fieldDataType = DataTypes.IntegerType;
                    break;
                case "float":
                    fieldDataType = DataTypes.FloatType;
                    break;
                case "double":
                    fieldDataType = DataTypes.DoubleType;
                    break;
                case "long":
                    fieldDataType = DataTypes.LongType;
                    break;
                case "boolean":
                    fieldDataType = DataTypes.BooleanType;
                    break;
                case "date":
                    fieldDataType = DataTypes.DateType;
                    break;
                case "short":
                case "char":
                case "string":
                case "varchar":
                default:
                    fieldDataType = DataTypes.StringType;
                    break;
            }
            structFields.add(DataTypes.createStructField(field.getKey(), fieldDataType, true));
        }
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> df = spark.createDataFrame(rdd1, structType);
        return SparkDataFrameMapping.getInstance().set(df);
    }
}
