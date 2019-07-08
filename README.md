#  Spark

## 安装spark(需要提前安装Scala)
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
