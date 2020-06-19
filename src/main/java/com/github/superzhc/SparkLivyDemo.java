package com.github.superzhc;

import com.github.superzhc.common.SparkSQL;
import com.github.superzhc.common.impl.SparkSQLImpl;
import com.github.superzhc.dataframe.SparkDataFrame;
import com.github.superzhc.dataframe.SparkDataFrameImpl;
import com.github.superzhc.livy.SparkLivy;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 2020年06月19日 superz add
 */
public class SparkLivyDemo
{
    public static void main(String[] args) {
        SparkLivy sparkLivy = new SparkLivy();
        SparkSQL sparkSQL = (SparkSQL) sparkLivy.wrapper(new SparkSQLImpl());

        // 读取关系型数据库
        String url = "jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false";
        String sql = "select * from test1";
        String dfKey = sparkSQL.jdbc(url, sql);// 读取表数据，获取数据的唯一标识
        
        // 读取hive
        String dfKey2=sparkSQL.hive(sql);// 对于hive数据库来说，spark读取的是Spark集群中配置的 $SPARK_HOME/conf/hive-site.xml 文件中配置的hive集群
        
        // 读取json字符串
        List<String> jsons = new ArrayList<>();
        jsons.add("{\"id\":\"1\",\"name\":\"zhangsan\"}");
        jsons.add("{\"id\":\"2\",\"name\":\"lisi\"}");
        jsons.add("{\"id\":\"3\",\"name\":\"wangwu\"}");
        String dfKey3 = sparkSQL.json(jsons);

        // 通过数据的唯一标识构建DataFrame
        SparkDataFrameImpl sparkDataFrame = new SparkDataFrameImpl(dfKey, "tableName");// 第二个参数设置关系表的名称
        SparkDataFrame df = (SparkDataFrameImpl) sparkLivy.wrapper(sparkDataFrame);

        System.out.println(df.printSchema());
        System.out.println("数据的条数：" + df.count());
        System.out.println(df.show());
        SparkDataFrame df0 = df.filter("name='lisi'");
        df = df.selectExpr("*", "id+10 b");
        df = df.rename("name", "xx");

        SparkDataFrame df2 = df.union(df.limit(1).key());
        SparkDataFrame df3 = df.join(df.limit(1).key(), "rowguid");

        // 内置函数
        df = df.execute("select name,base64(name) as a from testx", "testy");

        // 数据保存到新表中
        String tableName = "superz_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyMMddHHmmss"));
        // 若表不存在使用如下方法
        df.saveHive(tableName);
        df.saveJdbc("jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false", tableName, new Properties());

        // 若表已存在，使用如下方法
        df.saveHive(tableName, "append");
        df.saveJdbc("jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false", tableName, "append", new Properties());
    }
}
