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
