<h1 align="center"><a href="https://github.com/superzhc/SparkLivyExecuteor">SparkLivyExecuteor</a></h1>

[![![Build Status](https://travis-ci.org/superzhc/SparkLivyExecuteor.svg?branch=master)](https://travis-ci.org/superzhc/SparkLivyExecuteor)

使用 Livy 提交 Spark 任务

## 环境变量配置

该项目会读取两个环境变量，因此在使用之前需要进行配置，有如下 3 中配置方式：

- 直接配置在系统配置环境变量

- 命令行启动的时候配置环境变量，如下：

```bash
-Dlivy.url=http://127.0.0.1:18998 \                    # 配置实际的地址
-Dlivy.job.jars=D:\\code\\SparkLivyExecutor-0.0.1.jar  # 配置当前项目的jar包
```

- 代码中进行设置，如下：

```java
System.setProperty("livy.url", "http://127.0.0.1:18998");                    // 配置实际的地址
System.setProperty("livy.job.jars","D:\\code\\SparkLivyExecutor-0.0.1.jar"); // 配置当前项目的jar包
```

## 使用示例

对于 Spark 任务来说，我个人简单理解为：1、读取数据源；2、对获取到的 DataFrame 进行操作；3、落地存储。本项目也就是按照这样的步骤进行操作的。

**Spark 和 Livy 集成**

Livy 连接上 Spark 在本项目中是通过一个 **SparkLivy** 对外接口的，使用如下：

```java
SparkLivy sparkLivy = new SparkLivy();
```

我们的实际 Spark 逻辑都可以通过这个类来包装一下，可以让用户无感知 Livy 的操作。

> 后续使用的 sparkLivy 都是一个实例化。

### 1、读取数据源

我们读取数据源都在SparkSQL类中，如下：

```java
SparkSQL sparkSQL = sparkLivy.cglig(new SparkSQL());
```

目前支持的数据源有**关系型数据库**、**hive**、**json**、**parquet**、**csv**，简单示例如下：

**读取关系型数据库示例：**

```java
// 读取关系型数据库
String url = "jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false";
String sql = "select * from test1";
String dfKey = sparkSQL.jdbc(url, sql);// 读取表数据，获取数据的唯一标识
```

**读取hive数据源示例：**

```java
// 读取hive
String dfKey2=sparkSQL.hive(sql);// 对于hive数据库来说，spark读取的是Spark集群中配置的 $SPARK_HOME/conf/hive-site.xml 文件中配置的hive集群
```

**读取json字符串示例：**

```java
// 读取json字符串
List<String> jsons = new ArrayList<>();
jsons.add("{\"id\":\"1\",\"name\":\"zhangsan\"}");
jsons.add("{\"id\":\"2\",\"name\":\"lisi\"}");
jsons.add("{\"id\":\"3\",\"name\":\"wangwu\"}");
String dfKey3 = sparkSQL.json(jsons);
```

### 2、DataFrame的操作

通过读取数据源步骤，能获取到数据源的唯一标识，通过这个标识，我们就能构建出 SparkDataFrame，如下：

```java
// 通过数据的唯一标识构建DataFrame
SparkDataFrame sparkDataFrame = new SparkDataFrame(dfKey, "testx");// 第二个参数设置关系表的名称
SparkDataFrame df = sparkLivy.cglib(sparkDataFrame);
```

为了简化读取数据源并转换成 DataFrame，提供了一个 SparkLivyDao 工具类，使用如下：

```java
SparkLivyDao dao=new SparkLivyDao();
String url = "jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false";
String sql = "select * from test1";
SparkDataFrame df = dao.jdbc(url, sql,"testx");
```

#### printSchema

> 打印出数据的 Schema

```java
df.printSchema()
```

#### count

> 获取数据的条数

```java
df.count()
```

#### show

> 打印展示数据，默认是20条数据

```java
df.show()
```

#### execute

> 对 DataFrame 的表执行 sql 操作，获取到一个新的 DataFrame。

```java
df.execute("select *,length(name) lname from testx")
```

#### take

> 获取部分数据到本地

```java
Row[] rows = df.take(100);
for (Row row : rows) {
   System.out.println(row.toString());
}
```

#### 使用内置函数

```java
df.execute("select name,base64(name) as a from testx", "testy")
df.execute("select *,concat('name:',name) from testy")
```

### 3、存储

```java
// 数据保存到新表中
String tableName = "superz_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyMMddHHmmss"));
// 若表不存在使用如下方法
df.saveHive(tableName);
df.saveJdbc("jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false", tableName, new Properties());

// 若表已存在，使用如下方法
df.saveHive(tableName, "append");
df.saveJdbc("jdbc:mysql://127.0.0.1:3306/superz?user=root&password=123456&useSSL=false", tableName, "append", new Properties());
```

## 其他使用示例

### 复用 Livy 和 Spark 的连接

```java
// Livy和Spark的连接有一个sessionId，获取sessionId
int sessionId = sparkLivy.getSessionId();

// 复用Livy和Spark的连接
SparkLivy sparkLivy = new SparkLivy(sessionId);
```

> 建议：尽量复用连接，但每个连接在一定的有效期内没有操作的话会被回收掉。

### 获取SparkDataFrame实例对象的数据的唯一标识

```java
df.key()
```
