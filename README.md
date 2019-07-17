

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





















