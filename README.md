#  Spark

<<<<<<< HEAD
+ 内含模块

  Spark SQL

  Spark Streaming

  Spark mllib

  Spark graha

## RDD

弹性分布式数据集

安装spark(需要提前安装Scala
----------------

=======
## 安装spark(需要提前安装Scala)
>>>>>>> dc536a890bd0452413a83d6585867b853ce68546
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
<<<<<<< HEAD

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

=======
>>>>>>> dc536a890bd0452413a83d6585867b853ce68546
