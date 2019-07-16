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
