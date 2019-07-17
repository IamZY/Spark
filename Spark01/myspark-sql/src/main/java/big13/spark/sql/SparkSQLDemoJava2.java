package big13.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.codehaus.janino.Java;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 使用java实现Spark SQL 访问
 */
public class SparkSQLDemoJava2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("sparkSQL").master("local").enableHiveSupport().getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> rdd1 = sc.textFile("/user/zangyang.txt");

        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s).iterator();
            }
        });

        JavaRDD<Row> rdd3 = rdd2.map(new Function<String, Row>() {
            public Row call(String s) throws Exception {
                return RowFactory.create(s);
            }
        });


        // 构造表结构
        StructField[] fields = new StructField[1];
        fields[0] = new StructField("word", DataTypes.StringType,true, Metadata.empty());

        // 表结构类型
        StructType type = new StructType(fields);

        Dataset<Row> df = spark.createDataFrame(rdd3,type);

        df.createOrReplaceTempView("_doc");

        spark.sql("select word,count(*) from _doc group by word").show(1000,false);

    }
}
