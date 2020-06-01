package com.github.superzhc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.github.superzhc.dataframe.SparkDataFrameMapping;

/**
 * 2020年06月01日 superz add
 */
public class SparkOperateImpl extends AbstractSparkSession implements SparkOperate
{
    @Override
    public String jdbc(String url, String sql) {
        return jdbc(url, sql, new Properties());
    }

    @Override
    public String jdbc(String url, String sql, Properties props) {
        Dataset<Row> df = spark.read().format("jdbc").option("url", url).option("dbtable", sql).load();
        // 修复：Dataset并不能作为返回值返回，返回一个唯一标识
        // return df;
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

    @Override
    public String hive(String sql) {
        Dataset<Row> df = spark.sql(sql);
        // 修复：Dataset并不能作为返回值返回，返回一个唯一标识
        // return df;
        return SparkDataFrameMapping.getInstance().set(df);
    }

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
