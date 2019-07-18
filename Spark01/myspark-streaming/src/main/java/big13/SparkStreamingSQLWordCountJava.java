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
