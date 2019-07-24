package big13.ml;

import javafx.scene.chart.PieChart;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;

import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

public class LinearRegresRedWineDemo1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("lr");

        // SparkSession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaRDD<String> rdd1 = new JavaSparkContext(spark.sparkContext()).textFile("file:///E:/red.csv", 4);

        // 标签点rdd  对应标签和特征向量
        JavaRDD<LabeledPoint> rdd2 = rdd1.map(new Function<String, LabeledPoint>() {
            public LabeledPoint call(String s) throws Exception {
                String[] arr = s.split(";");
                Double label = Double.parseDouble(arr[11]);

                Vector v = Vectors.dense(
                        Double.parseDouble(arr[0]),
                        Double.parseDouble(arr[1]),
                        Double.parseDouble(arr[2]),
                        Double.parseDouble(arr[3]),
                        Double.parseDouble(arr[4]),
                        Double.parseDouble(arr[5]),
                        Double.parseDouble(arr[6]),
                        Double.parseDouble(arr[7]),
                        Double.parseDouble(arr[8]),
                        Double.parseDouble(arr[9]),
                        Double.parseDouble(arr[10])
                );

                return new LabeledPoint(label, v);
            }
        });

        Dataset<Row> df = spark.createDataFrame(rdd2, LabeledPoint.class);
//        df.show();

        Dataset<Row>[] arr =  df.randomSplit(new double[]{0.8, 0.2});

        Dataset<Row> trainSet = arr[0];
        Dataset<Row> testSet = arr[1];

        LinearRegression lr = new LinearRegression();
        lr.setMaxIter(3);

        LinearRegressionModel model = lr.train(trainSet);
        Dataset<Row> pre = model.transform(testSet);
        pre.show();

        // 将RDD转化为数据框
//        JavaRDD<Row> rdd3 = rdd2.map(new Function<Tuple2<Double, Vector>, Row>() {
//            public Row call(Tuple2<Double, Vector> v) throws Exception {
//                return RowFactory.create(v._1(), v._2());
//            }
//        });

//        StructField[] fields = new StructField[2];
//        fields[0] = new StructField("label", DataTypes.DoubleType,false, Metadata.empty());
//        fields[1] = new StructField("label", DataTypes.DoubleType,false, Metadata.empty());
//        StructType
//        spark.createDataFrame(rdd3)


    }
}
