

#  Spark

+ 内含模块

  Spark SQL

  Spark Streaming

  Spark mllib

  Spark graha

## spark集群模式
+ local
  	本地模式
    	不需要启动任何进程。同一jvm中使用多线程模拟.
+ standalone
  	独立模式
    	启动进程
    	master
    	worker
+ mesos
  	-
+ yarn

## RDD

弹性分布式数据集  是Spark的基本抽象 表示为不可变的 分区化的集合 可用于并行计算

该类包含filter persist map

对于kv类型的rdd 封装在JavaPairRDD

+ 内部五大属性
  + 分区列表
  + 计算某个切片的函数
  + 到其他RDD的依赖列表
  + (可选)针对kv类型RDD的分区类
  + (可选) 计算每个的首选的位置列表

+ 并行

  分布式 集群上多个节点同时执行计算任务

+ 并发

  通常指的是单个节点处理同时发起请求的能力

+ 三高

  高并发、高负载、高吞吐量

## RDD常见操作

rdd都是延迟计算  只有调用action方法的时候 才会触发job的提交

+ 变换

  map

  filter

  flatMap

  mapPartitions

  union

  distinct

  intersection

  reduceByKey

  groupByKey

  只要返回新的RDD 就是变换

+ action

  collect

  foreachPartition  需要连接数据库的时候使用这个方法

安装Spark(需要提前安装Scala)
----------------

+ 下载spark软件包
  	http://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
+ tar
  	$>tar -xzvf spark-2.4.0-bin-hadoop2.7.tgz -C /app
+ 创建软连接
  	$>cd /app
    	$>ln -sfT spark-2.4.0-bin-hadoop2.7 spark
+ 配置环境变量
  	$>sudo nano /etc/profile 
    	...
    	#spark
    	export SPARK_HOME=/hadoop/home/app/spark
    	export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
+ 生效环境变量
  	$>source /etc/profile
+ 验证spark
  	$>spark-shell

> sc.textFile("file:///home/hadoop/hello.txt").flatMap(_.split(" ")).map((__,1)).reduceByKey(_ ___+ ___)

## WordCount
+ Java
```Java
package big13.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.sys.Prop;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * wordcount
 * java
 */
public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCountJava");
        conf.setMaster("local");

//        Properties prop = new Properties();
//        prop.setProperty("hadoop.home.dir", "D:\\dev\\hadoop-2.7.3");
//        System.setProperties(prop);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("file:///d:/hello.txt");

        // 压缩
        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // 解压
        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        List<Tuple2<String, Integer>> list = rdd4.collect();

        for (Tuple2<String, Integer> t : list) {
            System.out.println(t._1 + ":::" + t._2);
        }

    }
}

```
+ Scala
```Scala
import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")
    // 创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("file:///d:/hello.txt")

    val rdd2 = rdd1.flatMap(_.split(" "))

    val rdd3 = rdd2.map((_, 1))

    val rdd4 = rdd3.reduceByKey((_ + _))

    val arr = rdd4.collect()

    arr.foreach(println(_))

  }
}

```

## 最高气温排序

```java
package big13.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;
import sun.awt.image.IntegerInterleavedRaster;

import java.util.Arrays;
import java.util.Iterator;

public class MaxTemp {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("MaxTemp");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("file:///d:/temp3.dat");


        JavaPairRDD<Integer,Integer> rdd2 = rdd1.mapToPair(new PairFunction<String, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(String s) throws Exception {
                String str[] = s.split(" ");
                return new Tuple2<Integer, Integer>(Integer.parseInt(str[0]),Integer.parseInt(str[1]));
            }
        });

        JavaPairRDD<Integer,Integer> rdd3 = rdd2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 > v2 ? v1:v2;
            }
        });
        // 按照key进行排序
//        JavaPairRDD<Integer,Integer> rdd4 = rdd3.sortByKey();
        // 按照其他属性排序
        JavaRDD<Tuple2<Integer,Integer>> rdd4 = rdd3.map(new Function<Tuple2<Integer, Integer>, Tuple2<Integer,Integer>>() {
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1) throws Exception {
                return v1;
            }
        });

        JavaRDD<Tuple2<Integer,Integer>> rdd5 = rdd4.sortBy(new Function<Tuple2<Integer, Integer>, Integer>() {
            public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
                return v1._2;
            }
        },false,1);


        List<Tuple2<Integer,Integer>> list = rdd5.collect();
//        list.sort(new Comparator<Tuple2<Integer, Integer>>() {
//            public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
//                return o1._1 - o2._1;
//            }
//        });

        for(Tuple2<Integer,Integer> t : list){
            System.out.println(t._1 + ":" + t._2);
        }


    }

}

```

## 气温聚合

```java
package big13.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TempAgg {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("TempAgg");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("file:///d:/temp3.dat");

        JavaPairRDD<Integer,ArrayList> rdd2 = rdd1.mapToPair(new PairFunction<String, Integer, ArrayList>() {
            public Tuple2<Integer, ArrayList> call(String s) throws Exception {
                String[] str = s.split(" ");

                Integer year = Integer.parseInt(str[0]);
                Integer max = Integer.parseInt(str[1]);
                Integer min = Integer.parseInt(str[1]);
                Double sum = Double.parseDouble(str[1]);
                Integer cnt = 1;
                Double avg = Double.parseDouble(str[1]);

                ArrayList arr = new ArrayList();
                arr.add(max);
                arr.add(min);
                arr.add(sum);
                arr.add(cnt);
                arr.add(avg);

                return new Tuple2<Integer, ArrayList>(year,arr);
            }
        });

        List<Tuple2<Integer,ArrayList>> rdd3 = rdd2.reduceByKey(new Function2<ArrayList, ArrayList, ArrayList>() {
            public ArrayList call(ArrayList a1, ArrayList a2) throws Exception {

                Integer max = Math.max(Integer.parseInt(a1.get(0).toString()),Integer.parseInt(a2.get(0).toString()));
                Integer min = Math.min(Integer.parseInt(a1.get(1).toString()),Integer.parseInt(a2.get(1).toString()));
                Double sum = Double.parseDouble(a1.get(2).toString()) + Double.parseDouble(a2.get(2).toString());
                Integer cnt = Integer.parseInt(a1.get(3).toString()) + Integer.parseInt(a2.get(3).toString());
                Double avg = sum / cnt;

                ArrayList arr = new ArrayList();
                arr.add(max);
                arr.add(min);
                arr.add(sum);
                arr.add(cnt);
                arr.add(avg);

                return arr;
            }
        }).sortByKey().collect();


        for (Tuple2<Integer,ArrayList> arr : rdd3){
            System.out.println(arr._1 + "::" + arr._2.toString());
        }


    }
}

```

## Spark SQL

与hive无缝集合

+ IDEA下进行spark sql开发，访问hive数据库

  + 创建scala模块，添加maven支持

    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>
        <groupId>big13</groupId>
        <artifactId>myspark-sql</artifactId>
        <version>1.0-SNAPSHOT</version>
    
        <dependencies>
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-sql_2.11</artifactId>
                <version>2.4.0</version>
            </dependency>
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-hive_2.11</artifactId>
                <version>2.4.0</version>
            </dependency>
            <dependency>
                <groupId>mysql</groupId>
                <artifactId>mysql-connector-java</artifactId>
                <version>5.1.17</version>
            </dependency>
        </dependencies>
    
    </project>
    ```

  + 复制core-site.xml , hdfs-site.xml , hive-site.xml文件到resources下

  + 需要配置访问权限

    点击Intellij IDEA 界面窗口Run，打开Edit Configuration，出现Run/Debug 
    Configurations界面。Application server 
    选择安装Tomcat所在的文件夹，点击Configuration一般自动配置好了，其他的就按照图上的填写。 

    > -DHADOOP_USER_NAME=zkpk

  + 编写scala代码

  + ```scala
    import org.apache.spark.sql.SparkSession
    
    object SparkSQLDemo2 {
      def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").enableHiveSupport().getOrCreate()
    
        val rdd1 = spark.sparkContext.textFile("/user/zangyang.txt")
    
        val rdd2 = rdd1.flatMap(_.split(" "))
        // 导入SparkSession的隐式转换
        import spark.implicits._
        // 将rdd转换成数据框
        val df = rdd2.toDF("word")
        // 将数据框注册成临时视图
        df.createOrReplaceTempView("_doc")
    
        spark.sql("select word,count(*) from _doc group by word").show(1000,false)
      }
    }
    
    ```

+ Java

  源数据：

  1,smith,12
  2,bob,13
  3,alex,14
  4,alice,15
  5,mike,16

  

  1,t001,100.9,1
  2,t002,132.9,1
  3,t001,123.9,2
  4,t003,111.9,2
  5,t001,121.9,2
  6,t003,102.9,4
  7,t002,105.9,3
  8,t001,104.9,3
  9,t002,130.9,3
  10,t002,120.9,4

  ```java
  package big13.spark.sql;
  
  import org.apache.spark.api.java.JavaRDD;
  import org.apache.spark.api.java.JavaSparkContext;
  import org.apache.spark.api.java.function.FlatMapFunction;
  import org.apache.spark.api.java.function.Function;
  import org.apache.spark.sql.Dataset;
  import org.apache.spark.sql.Row;
  import org.apache.spark.sql.RowFactory;
  import org.apache.spark.sql.SparkSession;
  import org.apache.spark.sql.types.DataTypes;
  import org.apache.spark.sql.types.Metadata;
  import org.apache.spark.sql.types.StructField;
  import org.apache.spark.sql.types.StructType;
  import org.codehaus.janino.Java;
  
  import java.util.Arrays;
  import java.util.Iterator;
  
  /**
   * 使用java实现Spark SQL 访问
   */
  public class SparkSQLDemoJava3 {
      public static void main(final String[] args) {
          SparkSession spark = SparkSession.builder().appName("sparkSQL").master("local").enableHiveSupport().getOrCreate();
          // 上下文
          JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
  
  
          // custs
          JavaRDD<String> c_rdd1 = sc.textFile("/user/custs.txt");
          JavaRDD<Row> c_rdd2 = c_rdd1.map(new Function<String, Row>() {
              public Row call(String s) throws Exception {
                  String[] arr = s.split(",");
                  Integer id = Integer.parseInt(arr[0]);
                  String name = arr[1];
                  Integer age = Integer.parseInt(arr[2]);
                  return RowFactory.create(id, name, age);
              }
          });
  
          StructField[] c_fileds = new StructField[3];
          c_fileds[0] = new StructField("id", DataTypes.IntegerType, false, Metadata.empty());
          c_fileds[1] = new StructField("name", DataTypes.StringType, false, Metadata.empty());
          c_fileds[2] = new StructField("age", DataTypes.IntegerType, false, Metadata.empty());
          StructType c_type = new StructType(c_fileds);
          Dataset<Row> c_df1 = spark.createDataFrame(c_rdd2, c_type);
          // 注册临时视图
          c_df1.createOrReplaceTempView("_custs");
  
          // order
          JavaRDD<String> o_rdd1 = sc.textFile("/user/order.txt");
          JavaRDD<Row> o_rdd2 = o_rdd1.map(new Function<String, Row>() {
              public Row call(String s) throws Exception {
                  String[] arr = s.split(",");
                  Integer id = Integer.parseInt(arr[0]);
                  String orderno = arr[1];
                  Float price = Float.parseFloat(arr[2]);
                  Integer cid = Integer.parseInt(arr[3]);
                  return RowFactory.create(id, orderno, price, cid);
              }
          });
  
          StructField[] o_fileds = new StructField[4];
          o_fileds[0] = new StructField("id", DataTypes.IntegerType, false, Metadata.empty());
          o_fileds[1] = new StructField("orderno", DataTypes.StringType, false, Metadata.empty());
          o_fileds[2] = new StructField("price", DataTypes.FloatType, false, Metadata.empty());
          o_fileds[3] = new StructField("cid", DataTypes.IntegerType, false, Metadata.empty());
          StructType o_type = new StructType(o_fileds);
          Dataset<Row> o_df1 = spark.createDataFrame(o_rdd2, o_type);
          o_df1.createOrReplaceTempView("_orders");
  
  
          /**************************/
  //        spark.sql("select * from _custs").show(1000,false);
  //        spark.sql("select * from _orders").show(1000,false);
  
  
          String sql = "select c.id,c.name,ifnull(o._sum,0) as totalPrice from _custs " +
                  "as c left outer join " +
                  "(select cid,sum(price) as _sum from _orders group by cid) as o on o.cid=c.id";
          spark.sql(sql).show(1000,false);
  
      }
  }
  
  ```

+ 将dataframe 写入 Json

  ```java
  import org.apache.spark.sql.SparkSession
  
  /**
    * 将DataFrame保存json
    */
  object SparkSQLJsonWriteDemo {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").enableHiveSupport().getOrCreate()
      val rdd1 = spark.sparkContext.textFile("/user/custs.txt")
      import spark.implicits._
      val df1 = rdd1.map(line=>{
        val arr = line.split(",")
        (arr(0).toInt,arr(1),arr(2).toInt)
      }).toDF("id","name","age")
      df1.show(1000,false)
      df1.where("id > 2").write.json("file:///e:/json")
    }
  }
  
  ```

+ 将Json数据转换成DataFrame

  ```java
  import org.apache.spark.sql.SparkSession
  
  /**
    * 加载Json数据成DataFrame
    */
  object SparkSQLJsonReadDemo {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").enableHiveSupport().getOrCreate()
      val df1 = spark.read.json("file:///e:/json")
      df1.show(1000,false)
    }
  }
  
  ```

+ parquet的读取与保存 （hive的线性存储）

+ mysql的读取与存储    (表不能存在)

  ```java
  import java.util.Properties
  
  import org.apache.spark.sql.SparkSession
  
  /**
    * 将DataFrame保存json
    */
  object SparkSQLJDBCWriteDemo {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").enableHiveSupport().getOrCreate()
      val rdd1 = spark.sparkContext.textFile("/user/custs.txt")
  
      import spark.implicits._
  
      val df1 = rdd1.map(line => {
        val arr = line.split(",")
        (arr(0).toInt, arr(1), arr(2).toInt)
      }).toDF("id", "name", "age")
  
      df1.show(1000, false)
      val url = "jdbc:mysql://localhost:3306.big13"
      var table = "custs"
      val prop = new Properties()
      prop.put("drivers", "com.mysql.jdbc.Driver")
      prop.put("user", "root")
      prop.put("password", "123456")
      df1.where("id > 2").write.jdbc(url, table, prop)
  
  
    }
  }
  
  ```

  ```java
  import java.util.Properties
  
  import org.apache.spark.sql.SparkSession
  
  /**
    * 将DataFrame保存json
    */
  object SparkSQLJBDCReadDemo {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").enableHiveSupport().getOrCreate()
  
      val url = "jdbc:mysql://localhost:3306/big13"
      var table = "custs"
      val prop = new Properties()
      prop.put("drivers", "com.mysql.jdbc.Driver")
      prop.put("user", "root")
      prop.put("password", "123456")
  
      val df1 = spark.read.jdbc(url, table, prop)
      df1.show(1000, false)
  
    }
  }
  
  ```

## 分布式分析引擎

+ 启动spark集群

+ 启动spark thriftserver
  服务器，接受客户端jdbc的请求。

  > $>/soft/spark/sbin/start-thriftserver.sh --master spark://s101:7077

+ 验证是否启动成功或者webui

  		> $>netstat -anop | grep 10000

+ 连接到thriftserver

  + beeline

    > $>/soft/spark/bi n/beeline
    > $beeline>!conn jdbc:hive2://localhost:10000/big12 ;
    >
    > $beeline>select * from orders ;

		4.2)编程API访问thriftserver
			a) pom.xml
		
		```xml
		<?xml version="1.0" encoding="UTF-8"?>
				<project xmlns="http://maven.apache.org/POM/4.0.0"
						 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
					 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
					<modelVersion>4.0.0</modelVersion>
				<groupId>big13</groupId>
				<artifactId>myspark-sql</artifactId>
			<version>1.0-SNAPSHOT</version>
		
				<dependencies>
					<dependency>
						<groupId>org.apache.spark</groupId>
						<artifactId>spark-sql_2.11</artifactId>
						<version>2.4.0</version>
					</dependency>
					<dependency>
						<groupId>org.apache.spark</groupId>
						<artifactId>spark-hive_2.11</artifactId>
						<version>2.4.0</version>
					</dependency>
					<dependency>
						<groupId>mysql</groupId>
						<artifactId>mysql-connector-java</artifactId>
						<version>5.1.17</version>
					</dependency>
					<dependency>
						<groupId>org.apache.hive</groupId>
						<artifactId>hive-jdbc</artifactId>
						<version>2.1.0</version>
					</dependency>
					<dependency>
						<groupId>org.apache.hive</groupId>
						<artifactId>hive-exec</artifactId>
						<version>2.1.0</version>
					</dependency>
			</dependencies>
		
		</project>
		
		
		```
	```
	
	```

b) java类

```java
package big13.spark.sql;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 使用Spar SQL分布式查询引擎
 */
public class SparkSQLThriftServerDemo1 {
	public static void main(String[] args) throws Exception {
		String driver = "org.apache.hive.jdbc.HiveDriver" ;
		Class.forName(driver);
		String url = "jdbc:hive2://s101:10000" ;

		Connection conn = DriverManager.getConnection(url) ;
		PreparedStatement ppst = conn.prepareStatement("select * from big12.orders") ;
		ResultSet rs = ppst.executeQuery() ;
		while(rs.next()){
			int id = rs.getInt("id") ;
			String orderno = rs.getString("orderno") ;
			float price = rs.getFloat("price") ;
			int cid = rs.getInt("cid") ;
			System.out.printf("%d/%s/%f/%d\r\n" , id , orderno , price ,cid);
		}
		rs.close();
		ppst.close();
		conn.close();
	}
}
```
## Spark Streaming

流计算模块 动态数据

准实时计算 内部原理通过小批次计算

+ Storm 实时性高 吞吐量小

rdd内分区 分区的数量控制

> spark.streaming.blockInterval=200m

限速

> conf.set("spark.streaming.receiver.maxRate","20")

动态调整接受速率

> conf.set("spark.steaming.backpressure.enabled","true")

### 步骤

+ 编写pom.xml
  			

```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" 		     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
					 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
				<modelVersion>4.0.0</modelVersion>
			<groupId>big13</groupId>
			<artifactId>myspark-streaming</artifactId>
			<version>1.0-SNAPSHOT</version>

			<dependencies>
				<dependency>
					<groupId>org.apache.spark</groupId>
					<artifactId>spark-streaming_2.11</artifactId>
					<version>2.4.0</version>
				</dependency>
			</dependencies>
</project>
```

+ Scala

```scala
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    // * 动态提取cpu核数
    conf.setMaster("local[*]")
    conf.setAppName("StreamingWordsCount")
    // 流上下文
    val ssc = new StreamingContext(conf, Seconds(2));
    val lines = ssc.socketTextStream("localhost", 8888)

    val words = lines.flatMap(_.split(" "))
    val pair = words.map((_, 1))

    val result = pair.reduceByKey(_ + _)
    result.print()
    // 启动上下文
    ssc.start()

    ssc.awaitTermination()

  }
}

```

+ 启动centos下的nc服务器

  > $>nc -lk 8888 

+ 启动流程序

+ 配置log4j.properties

  ```properties
  #
  # Licensed to the Apache Software Foundation (ASF) under one or more
  # contributor license agreements.  See the NOTICE file distributed with
  # this work for additional information regarding copyright ownership.
  # The ASF licenses this file to You under the Apache License, Version 2.0
  # (the "License"); you may not use this file except in compliance with
  # the License.  You may obtain a copy of the License at
  #
  #    http://www.apache.org/licenses/LICENSE-2.0
  #
  # Unless required by applicable law or agreed to in writing, software
  # distributed under the License is distributed on an "AS IS" BASIS,
  # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  # See the License for the specific language governing permissions and
  # limitations under the License.
  #
  
  # Set everything to be logged to the console
  log4j.rootCategory=ERROR, console
  log4j.appender.console=org.apache.log4j.ConsoleAppender
  log4j.appender.console.target=System.err
  log4j.appender.console.layout=org.apache.log4j.PatternLayout
  log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
  
  # Set the default spark-shell log level to WARN. When running the spark-shell, the
  # log level for this class is used to overwrite the root logger's log level, so that
  # the user can have different defaults for the shell and regular Spark apps.
  log4j.logger.org.apache.spark.repl.Main=WARN
  
  # Settings to quiet third party logs that are too verbose
  log4j.logger.org.spark_project.jetty=WARN
  log4j.logger.org.spark_project.jetty.util.component.AbstractLifeCycle=ERROR
  log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
  log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
  log4j.logger.org.apache.parquet=ERROR
  log4j.logger.parquet=ERROR
  
  # SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
  log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
  log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
  ```

+ Java版

  ```java
  package big13;
  
  import org.apache.spark.SparkConf;
  import org.apache.spark.api.java.function.FlatMapFunction;
  import org.apache.spark.api.java.function.Function;
  import org.apache.spark.api.java.function.Function2;
  import org.apache.spark.api.java.function.PairFunction;
  import org.apache.spark.streaming.Durations;
  import org.apache.spark.streaming.api.java.JavaDStream;
  import org.apache.spark.streaming.api.java.JavaPairDStream;
  import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
  import org.apache.spark.streaming.api.java.JavaStreamingContext;
  import scala.Tuple2;
  
  import java.util.Arrays;
  import java.util.Iterator;
  
  public class SparkStreamingWordCountJava {
      public static void main(String[] args) throws Exception {
          SparkConf conf = new SparkConf();
          conf.setAppName("SparkStreamingWordCount");
          conf.setMaster("local[*]");
          // 创建Java
          JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(2));
          // 创建套接字文本流
          JavaDStream<String> lines = ssc.socketTextStream("192.168.52.154", 8888);
  
          JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
              public Iterator<String> call(String s) throws Exception {
                  return Arrays.asList(s.split(" ")).iterator();
              }
          });
  
          JavaPairDStream<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
              public Tuple2<String, Integer> call(String s) throws Exception {
                  return new Tuple2<String, Integer>(s, 1);
              }
          });
  
  
          JavaPairDStream<String,Integer> result = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
              public Integer call(Integer v1, Integer v2) throws Exception {
                  return v1+v2;
              }
          });
  
          result.print();
  
          ssc.start();
          ssc.awaitTermination();
  
  
  
      }
  }
  
  ```

### windows操作

```scala
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWordCountWindowsScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    // * 动态提取cpu核数
    conf.setMaster("local[*]")
    conf.setAppName("StreamingWordsCount")
    // 流上下文
    val ssc = new StreamingContext(conf, Seconds(2));
    val lines = ssc.socketTextStream("192.168.52.154", 8888)

    val words = lines.flatMap(_.split(" "))
    val pair = words.map((_, 1))
	// rdd重复计算  4秒算一次 一次计算之前10秒的数据
    val result = pair.reduceByKeyAndWindow((a: Int, b: Int) => {
      a + b
    }, Seconds(10), Seconds(4))
    result.print()
    // 启动上下文
    ssc.start()

    ssc.awaitTermination()

  }
}

```

### updateStateByKey

计算历史数据 不消除之前的计算结果

+ code

  ```scala
  import org.apache.spark.SparkConf
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  
  object SparkStreamingWordCountUpdateStateByKeyScala {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      // * 动态提取cpu核数
      conf.setMaster("local[*]")
      conf.setAppName("StreamingWordsCount")
      // 流上下文
      val ssc = new StreamingContext(conf, Seconds(2));
      val lines = ssc.socketTextStream("192.168.52.154", 8888)
  
      val words = lines.flatMap(_.split(" "))
      val pair = words.map((_, 1))
  
  
      val result = pair.reduceByKey((a: Int, b: Int) => {
        a + b
      });
  
      def updateFunc(vs: Seq[Int], st: Option[Int]): Option[Int] = {
        var currCount = 0
        if (vs != null && !vs.isEmpty) {
          currCount = vs.sum
        }
        var newSt = currCount
        if (!st.isEmpty) {
          newSt = newSt + st.get
        }
        Some(newSt)
      }
  
      result.updateStateByKey(updateFunc).print()
  
      // 启动上下文
      ssc.start()
  
      ssc.awaitTermination()
  
    }
  }
  
  ```

+ 按key进行状态更新

  ```scala
  import org.apache.spark.SparkConf
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  
  import scala.collection.mutable.ArrayBuffer
  
  object SparkStreamingWordCountUpdateStateByKeyScala2 {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      // * 动态提取cpu核数
      conf.setMaster("local[*]")
      conf.setAppName("StreamingWordsCount")
      // 流上下文
      val ssc = new StreamingContext(conf, Seconds(2));
      val lines = ssc.socketTextStream("192.168.52.154", 8888)
  
      val words = lines.flatMap(_.split(" "))
      val pair = words.map((_, 1))
  
  
      val result = pair.reduceByKey((a: Int, b: Int) => {
        a + b
      });
  
      // 更新状态函数
      def updateFunc(vs: Seq[Int], st: Option[ArrayBuffer[(Long, Int)]]): Option[ArrayBuffer[(Long, Int)]] = {
        // 新状态
        val buf = new ArrayBuffer[(Long, Int)]()
        // 提取当前时间毫秒数
        var ms = System.currentTimeMillis()
        var currCount = 0
        // 当前v的数量
        if (vs != null && !vs.isEmpty) {
          currCount = vs.sum
        }
        //      var oldBuf = ArrayBuffer
        if (!st.isEmpty) {
          // 取出旧状态
          val oldBuf = st.get
          for (t <- oldBuf) {
            if (ms - t._1 < 10000) {
              buf.+=(t)
            }
          }
        }
        if (currCount != 0) {
          buf.+=((ms, currCount))
        }
  
        if (buf.isEmpty) {
          None
        } else {
          Some(buf)
        }
  
      }
  
      result.updateStateByKey(updateFunc).print()
  
      // 启动上下文
      ssc.start()
  
      ssc.awaitTermination()
  
    }
  }
  
  ```

### 保存文件

+ code

  ```scala
  import java.io.{File, FileOutputStream}
  import java.text.SimpleDateFormat
  import java.util.Date
  
  import org.apache.spark.SparkConf
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  
  import scala.collection.mutable.ArrayBuffer
  
  object SparkStreamingWordCountFileScala {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      // * 动态提取cpu核数
      conf.setMaster("local[*]")
      conf.setAppName("StreamingWordsCount")
      // 流上下文
      val ssc = new StreamingContext(conf, Seconds(2));
      val lines = ssc.socketTextStream("192.168.52.154", 8888)
  
      val words = lines.flatMap(_.split(" "))
      val pair = words.map((_, 1))
  
      val result = pair.reduceByKey(_ + _)
      // 输出大量的小文件
      // result.saveAsTextFiles("file:///e:/streaming","dat")
  
      // 每一个小时保存一次结果
      result.foreachRDD(rdd => {
        val rdd2 = rdd.mapPartitionsWithIndex((idx, it) => {
          val buf: ArrayBuffer[(Int, (String, Int))] = ArrayBuffer[(Int, (String, Int))]()
          for (t <- it) {
            buf.+=((idx, t))
          }
          buf.iterator
        })
  
        rdd2.foreachPartition(it => {
          val now = new Date()
          val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
          // 格式化时间串
          val strDate = sdf.format(now)
  
          val itt = it.take(1)
  
          if(!itt.isEmpty){
            val par = itt.next()._1
            val file = strDate + "-" + par + ".dat"
            // 需要手动创建文件夹 向文件中追加
            val fout = new FileOutputStream(new File("e:/stream", file),true)
            for (t <- it) {
              val word = t._2._1
              val cnt = t._2._2
              fout.write((word + "\t" + cnt + "\r\n").getBytes())
              fout.flush()
            }
  
            fout.close()
          }
        })
      })
  
      // 启动上下文
      ssc.start()
      ssc.awaitTermination()
  
    }
  }
  
  ```

## Spark Streaming + Spark SQL

+ code

  ```scala
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  
  /**
    *
   */
  object SparkStreamingSQLWordCountScala {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      // * 动态提取cpu核数
      conf.setMaster("local[*]")
      conf.setAppName("StreamingWordsCount")
  
      // SparkSession
      val spark = SparkSession.builder().config(conf).getOrCreate()
      // 流上下文
      val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
      val lines = ssc.socketTextStream("localhost", 8888)
  
      // 压扁变成单词流
      val words = lines.flatMap(lines=>{
        lines.split(" ")
      })
  
      words.foreachRDD(rdd=>{
        import spark.implicits._
        val df = rdd.toDF("word")
        df.createOrReplaceTempView("_doc")
        spark.sql("select word,count(*) from _doc group by word").show(1000,false)
      })
  
      // 启动上下文
      ssc.start()
  
      ssc.awaitTermination()
  
    }
  }
  
  ```

  ```java
  package big13;
  
  import org.apache.spark.SparkConf;
  import org.apache.spark.api.java.JavaPairRDD;
  import org.apache.spark.api.java.JavaRDD;
  import org.apache.spark.api.java.JavaSparkContext;
  import org.apache.spark.api.java.function.*;
  import org.apache.spark.sql.Dataset;
  import org.apache.spark.sql.Row;
  import org.apache.spark.sql.RowFactory;
  import org.apache.spark.sql.SparkSession;
  import org.apache.spark.sql.types.DataTypes;
  import org.apache.spark.sql.types.Metadata;
  import org.apache.spark.sql.types.StructField;
  import org.apache.spark.sql.types.StructType;
  import org.apache.spark.streaming.Durations;
  import org.apache.spark.streaming.Time;
  import org.apache.spark.streaming.api.java.JavaDStream;
  import org.apache.spark.streaming.api.java.JavaPairDStream;
  import org.apache.spark.streaming.api.java.JavaStreamingContext;
  import scala.Tuple2;
  
  import java.util.ArrayList;
  import java.util.Arrays;
  import java.util.Iterator;
  import java.util.List;
  
  public class SparkStreamingSQLWordCountJava {
      public static void main(final String[] args) throws Exception {
          SparkConf conf = new SparkConf();
          conf.setAppName("SparkStreamingWordCount");
          conf.setMaster("local[*]");
          // 创建Java
          final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
          JavaStreamingContext ssc = new JavaStreamingContext(new JavaSparkContext(spark.sparkContext()), Durations.seconds(2));
          // 创建套接字文本流
          JavaDStream<String> lines = ssc.socketTextStream("localhost", 8888);
  
          // 压扁
          JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
              public Iterator<String> call(String s) throws Exception {
                  return Arrays.asList(s.split(" ")).iterator();
              }
          });
  
          words.foreachRDD(new VoidFunction<JavaRDD<String>>() {
              public void call(JavaRDD<String> rdd) throws Exception {
                  JavaRDD<Row> rdd2 = rdd.map(new Function<String, Row>() {
                      public Row call(String s) throws Exception {
                          return RowFactory.create(s);
                      }
                  });
                  StructField[] fields = new StructField[1];
                  fields[0] = new StructField("word", DataTypes.StringType, true, Metadata.empty());
                  StructType type = new StructType(fields);
                  Dataset<Row> df = spark.createDataFrame(rdd2, type);
                  // 注册临时视图
                  df.createOrReplaceTempView("_doc");
                  spark.sql("select word,count(*) from _doc group by word").show(100, false);
              }
          });
  
  
          ssc.start();
          ssc.awaitTermination();
  
  
      }
  }
  
  ```

## Spark Streming + Kafka

+ Java Message Server（JMS）Java消息服务

+ 消息中间件

  + destination

    目的地

    队列 p2p

    主题 topic

  + P2P模型

    point to point

  + PubSub模型

    Publish Subscribe

+ kafka

  是消息中间件 可以当作很大的缓存

  kafka是消费组 组内只有一个消费者消费一条消息

  topic可以有多个分区

+ code

  ```scala
  package big13
  
  import org.apache.kafka.common.serialization.StringDeserializer
  import org.apache.spark.streaming.kafka010.KafkaUtils
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
  import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
  
  
  object SparkStreamingKafkaScala {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setAppName("SparkStreamingKafka")
      conf.setMaster("local[*]")
  
      val ssc = new StreamingContext(conf, Seconds(2))
  
      // kafka参数
      val kafkaParams = Map[String,Object](
        "bootstrap.servers"->"192.168.52.154:9092,192.168.52.155:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "g1",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
  
      val topics = Array("topic1")
      val stream = KafkaUtils.createDirectStream[String,String](
        ssc,
        // 首选一致性
        PreferConsistent,
        //
        Subscribe[String,String](topics,kafkaParams)
      )
  
      val kv = stream.map(rdd=>{
        (rdd.key(),rdd.value())
      })
  
      kv.print()
  
      ssc.start()
      ssc.awaitTermination()
  
  
    }
  }
  
  ```

+ 开启生产者

  > ./kafka-console-consumer.sh --zookeeper 192.168.52.154:2181 --topic topic1

+ 开启消费者

  > ./kafka-console-producer.sh --broker-list 192.168.52.154:9092 --topic topic1



## 线性回归

+ Java

  ```java
  package big13.ml;
  
  import javafx.scene.chart.PieChart;
  import org.apache.spark.SparkConf;
  import org.apache.spark.api.java.JavaRDD;
  import org.apache.spark.api.java.JavaSparkContext;
  import org.apache.spark.api.java.function.Function;
  import org.apache.spark.ml.feature.LabeledPoint;
  import org.apache.spark.ml.linalg.Vector;
  import org.apache.spark.ml.linalg.Vectors;
  
  import org.apache.spark.ml.regression.LinearRegression;
  import org.apache.spark.ml.regression.LinearRegressionModel;
  import org.apache.spark.sql.Dataset;
  import org.apache.spark.sql.Row;
  import org.apache.spark.sql.RowFactory;
  import org.apache.spark.sql.SparkSession;
  import org.apache.spark.sql.types.DataTypes;
  import org.apache.spark.sql.types.Metadata;
  import org.apache.spark.sql.types.StructField;
  import org.apache.spark.sql.types.StructType;
  import scala.Tuple2;
  
  public class LinearRegresRedWineDemo1 {
      public static void main(String[] args) {
          SparkConf conf = new SparkConf();
          conf.setMaster("local[*]").setAppName("lr");
  
          // SparkSession
          SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
          JavaRDD<String> rdd1 = new JavaSparkContext(spark.sparkContext()).textFile("file:///E:/red.csv", 4);
  
          // 标签点rdd  对应标签和特征向量
          JavaRDD<LabeledPoint> rdd2 = rdd1.map(new Function<String, LabeledPoint>() {
              public LabeledPoint call(String s) throws Exception {
                  String[] arr = s.split(";");
                  Double label = Double.parseDouble(arr[11]);
  
                  Vector v = Vectors.dense(
                          Double.parseDouble(arr[0]),
                          Double.parseDouble(arr[1]),
                          Double.parseDouble(arr[2]),
                          Double.parseDouble(arr[3]),
                          Double.parseDouble(arr[4]),
                          Double.parseDouble(arr[5]),
                          Double.parseDouble(arr[6]),
                          Double.parseDouble(arr[7]),
                          Double.parseDouble(arr[8]),
                          Double.parseDouble(arr[9]),
                          Double.parseDouble(arr[10])
                  );
  
                  return new LabeledPoint(label, v);
              }
          });
  
          Dataset<Row> df = spark.createDataFrame(rdd2, LabeledPoint.class);
  //        df.show();
  
          Dataset<Row>[] arr =  df.randomSplit(new double[]{0.8, 0.2});
  
          Dataset<Row> trainSet = arr[0];
          Dataset<Row> testSet = arr[1];
  
          LinearRegression lr = new LinearRegression();
          lr.setMaxIter(3);
  
          LinearRegressionModel model = lr.train(trainSet);
          Dataset<Row> pre = model.transform(testSet);
          pre.show();
  
      }
  }
  
  ```

## 逻辑回归

+ scala

  ```scala
  import org.apache.spark.SparkConf
  import org.apache.spark.ml.classification.LogisticRegression
  import org.apache.spark.ml.linalg.Vectors
  import org.apache.spark.sql.SparkSession
  
  /**
    *
    */
  object LogisticRegressionWhiteDemo1 {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setMaster("local[*]").setAppName("mlline")
  
      //创建SparkSession
      val spark = SparkSession.builder().config(conf).getOrCreate()
  
      val rdd1 = spark.sparkContext.textFile("file:///e:/white.csv");
  
      import spark.implicits._
  
      val df1 = rdd1.map(line=>{
        val arr = line.split(";")
        val label = if(arr(11).toDouble > 7) 1 else 0
        val vec = Vectors.dense(
          arr(0).toDouble,
          arr(1).toDouble,
          arr(2).toDouble,
          arr(3).toDouble,
          arr(4).toDouble,
          arr(5).toDouble,
          arr(6).toDouble,
          arr(7).toDouble,
          arr(8).toDouble,
          arr(9).toDouble,
          arr(10).toDouble
        )
  
        (label,vec)
      }).toDF("label","features")
  
  
      val Array(trainData,testData) = df1.randomSplit(Array(0.8,0.2))
  
      val lr = new LogisticRegression()
      lr.setMaxIter(3)
      val model = lr.fit(trainData)
  
      val ret = model.transform(testData)
  
      ret.show(100,false)
  
    }
  }
  
  ```

## 回归计算器

计算回归算法模型是否最好 

+ scala

  ```scala
  import org.apache.spark.SparkConf
  import org.apache.spark.ml.evaluation.RegressionEvaluator
  import org.apache.spark.ml.linalg.Vectors
  import org.apache.spark.ml.regression.LinearRegression
  import org.apache.spark.sql.SparkSession
  
  /**
    * 评测线性回归模型好坏
    */
  object SparkMLLineRegressEvaluatorScala {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setMaster("local[*]").setAppName("mlline")
  
      //创建SparkSession
      val spark = SparkSession.builder().config(conf).getOrCreate()
  
      //1.定义样例类
      case class Wine(FixedAcidity: Double,
                      VolatileAcidity: Double,
                      CitricAcid: Double,
                      ResidualSugar: Double,
                      Chlorides: Double,
                      FreeSulfurDioxide: Double,
                      TotalSulfurDioxide: Double,
                      Density: Double,
                      PH: Double,
                      Sulphates: Double,
                      Alcohol: Double,
                      Quality: Double)
  
      //2.加载csv红酒文件，变换形成rdd
      val file = "file:///E:\\red.csv";
      val wineDataRDD = spark.sparkContext.textFile(file)
        .map(line => {
          val w = line.split(";")
          Wine(w(0).toDouble,
            w(1).toDouble,
            w(2).toDouble,
            w(3).toDouble,
            w(4).toDouble,
            w(5).toDouble,
            w(6).toDouble,
            w(7).toDouble,
            w(8).toDouble,
            w(9).toDouble,
            w(10).toDouble,
            w(11).toDouble)
        }
        )
  
  
      //导入sparksession的隐式转换对象的所有成员，才能将rdd转换成Dataframe
      import spark.implicits._
  
      //创建数据框,变换成(double , Vector)二元组
      //训练数据集
      val trainingDF = wineDataRDD.map(w =>
        (w.Quality,
          Vectors.dense(
            w.FixedAcidity,
            w.VolatileAcidity,
            w.CitricAcid,
            w.ResidualSugar,
            w.Chlorides,
            w.FreeSulfurDioxide,
            w.TotalSulfurDioxide,
            w.Density,
            w.PH,
            w.Sulphates,
            w.Alcohol)
        )
      ).toDF("label", "features")
  
      trainingDF.show(100, false)
  
      //3.创建线性回归对象
      val lr = new LinearRegression()
  
      //4.设置回归对象参数
      lr.setMaxIter(2)
      //
      //5.拟合模型,训练模型
      val model = lr.fit(trainingDF)
      //
      //6.构造测试数据集
      val testDF = spark.createDataFrame(Seq(
        (5.0, Vectors.dense(7.4, 0.7, 0.0, 1.9, 0.076, 25.0, 67.0,
          0.9968, 3.2, 0.68, 9.8)),
        (5.0, Vectors.dense(7.8, 0.88, 0.0, 2.6, 0.098, 11.0, 34.0,
          0.9978, 3.51, 0.56, 9.4)),
        (7.0, Vectors.dense(7.3, 0.65, 0.0, 1.2, 0.065, 15.0, 18.0,
          0.9968, 3.36, 0.57, 9.5))))
        .toDF("label", "features")
      //7.对测试数据集注册临时表
      testDF.createOrReplaceTempView("test")
      //8.对测试数据应用模型
      val result = model.transform(testDF)
  //    result.show(100, false)
  
      // 创建回归计算器
      val e = new RegressionEvaluator()
      e.setLabelCol("label")
      e.setPredictionCol("prediction")
      // 均方根
      e.setMetricName("rmse")
      // 1.0190808640946731 越接近0越好
      // √[∑di^2/n]=Re
      val rmse = e.evaluate(result)
      println(rmse)
  
    }
  }
  
  ```

## 贝叶斯

## MuticlassClassficationEvaluator

![image](https://github.com/IamZY/Spark/blob/master/image/01.png)

+ code

  ```scala
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
  
  /**
    * 多元分类模型计算器
    */
  object MuticlassEvaluationScala {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setMaster("local[*]").setAppName("mlline")
  
      //创建SparkSession
      val spark = SparkSession.builder().config(conf).getOrCreate()
      val rdd1 = spark.sparkContext.makeRDD(Array(
        (1, 1), (1, 1), (1, 0), (1, 1), (1, 1), (0, 0), (0, 1), (0, 0), (0, 1), (0, 0)
      ))
  
      import spark.implicits._
  
      val df = rdd1.map(t => (t._1.toDouble, t._2.toDouble)).toDF("label", "prediction")
  
      df.show(100, false)
  
      val e = new MulticlassClassificationEvaluator()
      e.setLabelCol("label")
      e.setPredictionCol("prediction")
      // f1,accuracy,weightedRecall
      e.setMetricName("weightedPrecision")
      val ret = e.evaluate(df)
  
      // 0.7083333333333333
      println(ret)
  
    }
  }
  
  ```

## TF和IDF

词频 term frequence 

逆文档频率 Inverse document frequence  	$D/d$	= $文章总数/出现单词文章个数$

![images](https://github.com/IamZY/Spark/blob/master/image/02.png)

+ code

  ```scala
  import org.apache.spark.SparkConf
  import org.apache.spark.ml.classification.LogisticRegression
  import org.apache.spark.ml.feature.{HashingTF, LabeledPoint}
  import org.apache.spark.ml.linalg.SparseVector
  import org.apache.spark.sql.SparkSession
  
  
  /**
    * 垃圾邮件分类
    */
  object SpamFilterScala4ML {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setAppName("ml_linearRegress")
      conf.setMaster("local[*]")
  
      //创建SparkSession
      val spark = SparkSession.builder().config(conf).getOrCreate()
      import spark.implicits._
      //哈希词频
      val tf = new HashingTF()
      tf.setInputCol("words")
      tf.setOutputCol("features")
      tf.setNumFeatures(1000)
  
      //垃圾邮件
      val rdd10 = spark.sparkContext.textFile("e:\\spam.txt")
      val rdd11 = rdd10.map(_.split(" "))
      val df10 = rdd11.toDF("words")
  
      val df11 = tf.transform(df10).map(r => {
        new LabeledPoint(1, r.getAs[SparseVector]("features"))
      })
      println("===spam====")
      df11.show(10, false)
  
  
      //常规邮件
      val rdd20 = spark.sparkContext.textFile("e:\\normal.txt")
      val rdd21 = rdd20.map(_.split(" "))
      val df20 = rdd21.toDF("words")
  
      val df21 = tf.transform(df20).map(r => {
        new LabeledPoint(0, r.getAs[SparseVector]("features"))
      })
      println("===normal====")
      df21.show(100, false)
  
  
      val sample = df11.union(df21)
  
      println("===sample====")
      sample.show(1000, false)
  
      //切割数据
      val Array(train, test) = sample.randomSplit(Array(0.8, 0.2))
      val lr = new LogisticRegression()
      val model = lr.fit(train)
      val result = model.transform(test)
      result.show(1000, false)
  
    }
  }
  
  ```

## Kmeans









































