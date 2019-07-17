package big13.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 使用java实现Spark SQL 访问
 */
public class SparkSQLDemoJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("sparkSQL").master("local").enableHiveSupport().getOrCreate();

//        Dataset<Row> df = spark.sql("use hive");
    }
}
