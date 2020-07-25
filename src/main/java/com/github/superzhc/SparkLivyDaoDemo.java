package com.github.superzhc;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.superzhc.spark.SparkDataFrame;
import com.github.superzhc.utils.ProjectPackageTool;

/**
 * 2020年06月19日 superz add
 */
public class SparkLivyDaoDemo
{
    public static void main(String[] args) {
        Integer sessionId = null;
        sessionId=894;
        SparkLivyDao dao;
        if (null == sessionId) {
            ProjectPackageTool.doPackage();
            dao = new SparkLivyDao();
        }
        else {
            dao = new SparkLivyDao(sessionId);
        }

        // 读取关系型数据库
//        String url = "jdbc:mysql://192.168.186.13:3306/superz?user=root&password=Gepoint&useSSL=false";
//        String sql = "select * from test1";
//        String url = "jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false";
        String url="jdbc:mysql://192.168.186.13:3306/bigdata_scene03_rktj?user=root&password=Gepoint&useSSL=false";
        String sql="select * from t_rk_baseinfo_5kw";

//        SparkDataFrame df = dao.jdbc(url, sql,"testx");
        // 分区读取，整数类型、时间戳类型进行分区
        SparkDataFrame df=dao.jdbc4Partition(url,sql,"t_rk_baseinfo_5kw","row_id",4214890L,56097639L,100);
        System.out.println(df.key());
        System.out.println(df.getNumPartitions());
        System.out.println(df.printSchema());
//        System.out.println("数据的条数：" + df.count());
//        System.out.println(df.show());

//        df=df.filter("name='徐平' and telephone='13773258168'");

        // 自定义函数使用
//        SparkDataFrame df5 = df.cast("d2", "date");
//        System.out.println(df5.printSchema());

//        df=df.drop("name","d1","d2");
        // 自定义函数使用
//        df = df.dateformat("d1", "d1", "yyyy/MM/dd");
//        System.out.println(df.show());
//        df = df.strdateformat("d2", "d3", "yyyyMMdd", "yyyy-MM-dd");
//        System.out.println(df.show());
//        df = df.substring("name", "n2", 1, 3);
//        System.out.println(df.show());

//        String items = "{\"1\":\"男\",\"2\":\"女\"}";
//        df = df.codeitem("sex", "sex2", items);
//        System.out.println(df.show());

        // df=df.groupBy(new String[]{"sex"},new String[]{"sex","min(rowguid) as min","max(rowguid) as max","sum(rowguid) as sum","avg(rowguid) as avg"});

//        Map<String, String> map = new HashMap<>();
//        map.put("*","count");
//        map.put("rowguid", "min");
//        map.put("rowguid", "max");
//        map.put("rowguid", "sum");
//        map.put("rowguid", "avg");
//        SparkDataFrame df2 = df.groupBy(new String[] {"sex" }, map);
//        System.out.println(df2.show());
        System.out.println(df.show(100));
        System.out.println(df.count());

        // 读取hive
//        SparkDataFrame df2=dao.hive("select * from superz_test","testy");// 对于hive数据库来说，spark读取的是Spark集群中配置的 $SPARK_HOME/conf/hive-site.xml 文件中配置的hive集群
//        System.out.println(df2.show(10));
//        String codeitems="{\"2\":\"代码项1\",\"33\":\"代码项2\",\"3333\":\"代码项3\"}";
//        df2=df2.codeitem("name","name2",codeitems);
//        System.out.println(df2.show());

        // 读取json字符串
//        List<String> jsons = new ArrayList<>();
//        jsons.add("{\"id\":\"1\",\"name\":\"zhangsan\"}");
//        jsons.add("{\"id\":\"2\",\"name\":\"lisi\"}");
//        jsons.add("{\"id\":\"3\",\"name\":\"wangwu\"}");
//        SparkDataFrame df3 = dao.json(jsons,"testz");
//        System.out.println(df3.show());
//
//        df3 = df3.execute("select *,length(name) as ln from testz");
//        System.out.println(df3.show());
//
//        // 值运算
//        df3 = df3.selectExpr("*","ln+id as lnid", "ln+10 as ln10");
//        System.out.println(df3.show());
//
//        SparkDataFrame df4=df3.rename("ln","ln2");
//        System.out.println(df4.show());

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
