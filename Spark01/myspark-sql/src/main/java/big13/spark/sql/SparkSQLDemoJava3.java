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
