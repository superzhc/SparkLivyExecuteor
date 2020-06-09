<h1 align="center"><a href="https://github.com/superzhc/SparkLivyExecuteor">SparkLivyExecuteor</a></h1>

使用Livy提交Spark任务

## 使用示例

```java
System.setProperty("livy.url", "http://127.0.0.1:18998");// 配置实际的地址
System.setProperty("livy.job.jars","D:\\code\\SparkLivyExecutor-0.0.1.jar");// 配置LivySparkLab_Simple包的实际地址
//        String url = "jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false";
String url="";
Integer sessionId = null;
//        sessionId = 797;
SparkDao dao;
if (null == sessionId)
   dao = new SparkDao(url);// 新建一个连接
else
   dao = new SparkDao(url, sessionId);// 复用原有的连接
// 执行 mysql数据库
//        SparkDataFrame df = dao.query("select * from test1", "testx");
// 执行hive
SparkDataFrame df = dao.query("select * from superz_test where name='3333'", "testx");

System.out.println("数据的条数：" + df.count());
// 获取数据到本地
Row[] rows = df.take(100);
for (Row row : rows) {
   System.out.println(row.toString());
}
SparkDataFrame df2 = df.execute("select id,name from testx");
List<Row> rows1 = df.takeAsList(100);
for (Row row : rows1) {
   System.out.println(row.toString());
}

// 数据保存到新表中
String tableName = "superz_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyMMddHHmmss"));
// 若表不存在使用如下方法
df.saveHive(tableName);
df.saveJdbc("jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false", tableName,
       new Properties());
// 若表已存在，使用如下方法
df.saveHive(tableName, "append");
df.saveJdbc("jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false", tableName,
       "append", new Properties());
```
