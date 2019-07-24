package big13.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 朴素贝叶斯分类 白酒质量预测
 */
public class NaiveBayesWhiteJava {
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
                Double label = Double.parseDouble(arr[11]) > 5.5 ? 1d : 0d;

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

        Dataset<Row>[] arr = df.randomSplit(new double[]{0.7, 0.3}, 1234L);

        Dataset<Row> trainSet = arr[0];
        Dataset<Row> testSet = arr[1];

        NaiveBayes bayes = new NaiveBayes();
        NaiveBayesModel model = bayes.fit(trainSet);

        Dataset<Row> ret = model.transform(testSet);

        ret.select("label","prediction").show();


    }
}
