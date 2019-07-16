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
import java.util.Comparator;
import java.util.List;

/**
 * 标签生成
 */
public class TaggenJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("Taggen");
        // 加载文件
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("file:///d:/source/temptags.txt");

        // 解析变化
        JavaPairRDD<String, List<String>> rdd2 = rdd1.mapToPair(new PairFunction<String, String, List<String>>() {
            public Tuple2<String, List<String>> call(String s) throws Exception {
                String[] arr = s.split("\t");
                String busid = arr[0];
                String json = arr[1];
                List<String> tags = TagUtils.parseJson(json);
                return new Tuple2<String, List<String>>(busid, tags);
            }
        });

        JavaPairRDD<String, List<String>> rdd3 = rdd2.filter(new Function<Tuple2<String, List<String>>, Boolean>() {
            public Boolean call(Tuple2<String, List<String>> t) throws Exception {
                return (t._2 != null && !t._2.isEmpty());
            }
        });

        // 压扁
        JavaPairRDD<String, String> rdd4 = rdd3.flatMapValues(new Function<List<String>, Iterable<String>>() {
            public Iterable<String> call(List<String> v1) throws Exception {
                return v1;
            }
        });


        JavaPairRDD<Tuple2<String, String>, Integer> rdd5 = rdd4.mapToPair(new PairFunction<Tuple2<String, String>, Tuple2<String, String>, Integer>() {
            public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, String> v1) throws Exception {
                return new Tuple2<Tuple2<String, String>, Integer>(v1, 1);
            }
        });


        JavaPairRDD<Tuple2<String, String>, Integer> rdd6 = rdd5.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        
        JavaPairRDD<String, Tuple2<String, Integer>> rdd7 = rdd6.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String,Integer>>() {
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> t) throws Exception {
                return new Tuple2<String, Tuple2<String, Integer>>(t._1._1,new Tuple2<String, Integer>(t._1._2,t._2));
            }
        });

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> rdd8 = rdd7.groupByKey();

        JavaPairRDD<String, List<Tuple2<String, Integer>>> rdd9 = rdd8.mapValues(new Function<Iterable<Tuple2<String, Integer>>, List<Tuple2<String,Integer>>>() {
            public List<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> it) throws Exception {
                List<Tuple2<String,Integer>> newList = new ArrayList<Tuple2<String, Integer>>();
                for (Tuple2<String,Integer> t : it){
                    newList.add(t);
                }

                newList.sort(new Comparator<Tuple2<String, Integer>>() {
                    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                        return -(o1._2 - o2._2);
                    }
                });
                return newList;
            }
        });

//        JavaPairRDD<String, List<Tuple2<String, Integer>>> rdd10 = rdd9.map(new Function<Tuple2<String, List<Tuple2<String, Integer>>>, Tuple2<String, List<Tuple2<String, Integer>>>>() {
//            @Override
//            public Tuple2<String, List<Tuple2<String, Integer>>> call(Tuple2<String, List<Tuple2<String, Integer>>> v2) throws Exception {
//                return v2;
//            }
//        });

        List list = rdd4.collect();


        for (Object o : list) {
            System.out.println(o);
        }


    }
}
