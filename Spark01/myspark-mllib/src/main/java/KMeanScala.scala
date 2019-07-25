import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.SparkSession

/**
  * kmean监督
  */
object KMeanScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("mlline")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val rdd = spark.read.format("libsvm").load("file:///e:/sample_kmeans_data.txt")
    val kmeans = new KMeans()

    kmeans.setK(2).setSeed(1L)
    val model = kmeans.fit(rdd)

    import spark.implicits._

    val test = spark.sparkContext.makeRDD(Array(
      new LabeledPoint(7, Vectors.dense(1.1, 1.2, 1.3)),
      new LabeledPoint(8, Vectors.dense(8.9, 8.8, 8.7))
    )).toDF("label", "features")

    val ret = model.transform(test)
    ret.show(100, false)


  }
}
