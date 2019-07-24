import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

/**
  * 评测线性回归模型好坏
  */
object SparkMLLineRegressEvaluatorScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("mlline")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //1.定义样例类
    case class Wine(FixedAcidity: Double,
                    VolatileAcidity: Double,
                    CitricAcid: Double,
                    ResidualSugar: Double,
                    Chlorides: Double,
                    FreeSulfurDioxide: Double,
                    TotalSulfurDioxide: Double,
                    Density: Double,
                    PH: Double,
                    Sulphates: Double,
                    Alcohol: Double,
                    Quality: Double)

    //2.加载csv红酒文件，变换形成rdd
    val file = "file:///E:\\red.csv";
    val wineDataRDD = spark.sparkContext.textFile(file)
      .map(line => {
        val w = line.split(";")
        Wine(w(0).toDouble,
          w(1).toDouble,
          w(2).toDouble,
          w(3).toDouble,
          w(4).toDouble,
          w(5).toDouble,
          w(6).toDouble,
          w(7).toDouble,
          w(8).toDouble,
          w(9).toDouble,
          w(10).toDouble,
          w(11).toDouble)
      }
      )


    //导入sparksession的隐式转换对象的所有成员，才能将rdd转换成Dataframe
    import spark.implicits._

    //创建数据框,变换成(double , Vector)二元组
    //训练数据集
    val trainingDF = wineDataRDD.map(w =>
      (w.Quality,
        Vectors.dense(
          w.FixedAcidity,
          w.VolatileAcidity,
          w.CitricAcid,
          w.ResidualSugar,
          w.Chlorides,
          w.FreeSulfurDioxide,
          w.TotalSulfurDioxide,
          w.Density,
          w.PH,
          w.Sulphates,
          w.Alcohol)
      )
    ).toDF("label", "features")

    trainingDF.show(100, false)

    //3.创建线性回归对象
    val lr = new LinearRegression()

    //4.设置回归对象参数
    lr.setMaxIter(2)
    //
    //5.拟合模型,训练模型
    val model = lr.fit(trainingDF)
    //
    //6.构造测试数据集
    val testDF = spark.createDataFrame(Seq(
      (5.0, Vectors.dense(7.4, 0.7, 0.0, 1.9, 0.076, 25.0, 67.0,
        0.9968, 3.2, 0.68, 9.8)),
      (5.0, Vectors.dense(7.8, 0.88, 0.0, 2.6, 0.098, 11.0, 34.0,
        0.9978, 3.51, 0.56, 9.4)),
      (7.0, Vectors.dense(7.3, 0.65, 0.0, 1.2, 0.065, 15.0, 18.0,
        0.9968, 3.36, 0.57, 9.5))))
      .toDF("label", "features")
    //7.对测试数据集注册临时表
    testDF.createOrReplaceTempView("test")
    //8.对测试数据应用模型
    val result = model.transform(testDF)
    //    result.show(100, false)

    // 创建回归计算器
    val e = new RegressionEvaluator()
    e.setLabelCol("label")
    e.setPredictionCol("prediction")
    // 均方根
    e.setMetricName("rmse")
    // 1.0190808640946731 越接近0越好
    // √[∑di^2/n]=Re
    val rmse = e.evaluate(result)
    println(rmse)

  }
}
