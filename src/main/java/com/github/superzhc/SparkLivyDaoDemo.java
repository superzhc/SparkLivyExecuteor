package com.github.superzhc;

import java.util.ArrayList;
import java.util.List;

import com.github.superzhc.spark.SparkDataFrame;

/**
 * 2020年06月19日 superz add
 */
public class SparkLivyDaoDemo
{
    public static void main(String[] args) {
        SparkLivyDao dao=new SparkLivyDao(860);
        System.out.println(dao.sessionId());

        // 读取关系型数据库
        String url = "jdbc:mysql://192.168.186.13:3306/superz?user=root&password=Gepoint&useSSL=false";
//        String url = "jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false";
        String sql = "select * from test1";
        SparkDataFrame df = dao.jdbc(url, sql,"testx");
        System.out.println(df.key());
        System.out.println(df.printSchema());
        System.out.println("数据的条数：" + df.count());
        System.out.println(df.show());

        // 读取hive
        SparkDataFrame df2=dao.hive("select * from superz_test","testy");// 对于hive数据库来说，spark读取的是Spark集群中配置的 $SPARK_HOME/conf/hive-site.xml 文件中配置的hive集群
        System.out.println(df2.show(10));

        // 读取json字符串
        List<String> jsons = new ArrayList<>();
        jsons.add("{\"id\":\"1\",\"name\":\"zhangsan\"}");
        jsons.add("{\"id\":\"2\",\"name\":\"lisi\"}");
        jsons.add("{\"id\":\"3\",\"name\":\"wangwu\"}");
        SparkDataFrame df3 = dao.json(jsons,"testz");
        System.out.println(df3.show());

//        // 数据保存到新表中
//        String tableName = "superz_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyMMddHHmmss"));
//        // 若表不存在使用如下方法
//        df.saveHive(tableName);
//        df.saveJdbc("jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false", tableName, new Properties());
//
//        // 若表已存在，使用如下方法
//        df.saveHive(tableName, "append");
//        df.saveJdbc("jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false", tableName, "append", new Properties());
    }
}
