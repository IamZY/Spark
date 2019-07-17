package big13;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkStreamingWordCountJava2 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingWordCount");
        conf.setMaster("local[*]");
        // 创建Java
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(2));
        // 创建套接字文本流
        JavaDStream<String> lines = ssc.socketTextStream("192.168.52.154", 8888);

        JavaPairDStream<String, Integer> pairs = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
                String arr[] = s.split(" ");
                for (String a : arr) {
                    list.add(new Tuple2<String, Integer>(s, 1));
                }

                return list.iterator();
            }
        });

        JavaPairDStream<String, Integer> result = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

//        result.saveAsHadoopFiles("e:/streaming", "dat");
        result.print();
        ssc.start();
        ssc.awaitTermination();


    }
}
