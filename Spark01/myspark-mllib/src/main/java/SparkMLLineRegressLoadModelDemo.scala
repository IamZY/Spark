import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.SparkSession


/**
  * 线性回归 读取保存的模型
  */
object SparkMLLineRegressLoadModelDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("mlline")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val rdd1 = spark.sparkContext.textFile("file:///e:/red.csv")

    import spark.implicits._

    val df = rdd1.map(line => {
      val arr = line.split(";")
      val label = arr(11).toDouble
      val vec = Vectors.dense(
        arr(0).toDouble,
        arr(1).toDouble,
        arr(2).toDouble,
        arr(3).toDouble,
        arr(4).toDouble,
        arr(5).toDouble,
        arr(6).toDouble,
        arr(7).toDouble,
        arr(8).toDouble,
        arr(9).toDouble,
        arr(10).toDouble
      )
      (label, vec)
    }).toDF("label", "features")

    // 按照28分切割样本集 形成训练集和测试集
    val Array(trainData, testData) = df.randomSplit(Array[Double](0.8, 0.2))
    println("============= 训练集 =============")
    trainData.show(100,false)
    println("============= 测试集 =============")
    testData.show(100,false)

    val model = LinearRegressionModel.load("file:///e:/model")
    val result = model.transform(testData)

    result.show(10,false)
  }
}
