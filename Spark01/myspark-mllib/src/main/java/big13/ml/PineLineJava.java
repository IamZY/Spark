package big13.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.List;

/**
 * java 管线化处理
 */
public class PineLineJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("lr");
        conf.setMaster("local[*]");

        /**
         *a b c d e spark", 1.0),
         "b d", 0.0),
         "spark f g h", 1.0),
         "hadoop mapreduce", 0.0)
         */
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext()) ;

        List<Tuple2<Double,String>> list = new ArrayList<Tuple2<Double, String>>() ;
        list.add(new Tuple2<Double, String>(1.0, "a b c d e spark")) ;
        list.add(new Tuple2<Double, String>(0.0, "b d")) ;
        list.add(new Tuple2<Double, String>(1.0, "spark f g h")) ;
        list.add(new Tuple2<Double, String>(1.0, "hadoop mapreduce")) ;
        //rdd1
        JavaRDD<Tuple2<Double, String>> rdd1 = sc.parallelize(list) ;
        //rdd2
        JavaRDD<Row> rdd2 = rdd1.map(new Function<Tuple2<Double,String>, Row>() {
            public Row call(Tuple2<Double, String> v1) throws Exception {
                return RowFactory.create(v1._1 ,v1._2);
            }
        }) ;

        //转换成Dataframe
        StructField[] fields = new StructField[2] ;
        fields[0] = new StructField("label" , DataTypes.DoubleType , false , Metadata.empty()) ;
        fields[1] = new StructField("text" , DataTypes.StringType , false , Metadata.empty()) ;
        StructType type = new StructType(fields) ;
        Dataset<Row> df1 = spark.createDataFrame(rdd2 , type) ;

        //分词
        Tokenizer token = new Tokenizer() ;
        token.setInputCol("text") ;
        token.setOutputCol("words") ;

        //停用词
        StopWordsRemover stop = new StopWordsRemover() ;
        stop.setInputCol(token.getOutputCol()) ;
        stop.setOutputCol("words2") ;

        //哈希词频
        HashingTF tf = new HashingTF() ;
        tf.setNumFeatures(1000) ;
        tf.setInputCol(stop.getOutputCol()) ;
        tf.setOutputCol("features") ;

        //逻辑回归
        LogisticRegression lr = new LogisticRegression() ;
        lr.setMaxIter(3) ;

        //管线
        Pipeline pl = new Pipeline() ;
        PipelineStage[] stages = new PipelineStage[4] ;
        stages[0] = token ;
        stages[1] = stop ;
        stages[2] = tf;
        stages[3] = lr;
        pl.setStages(stages) ;

        //拟合模型
        PipelineModel model = pl.fit(df1) ;


        List<Tuple2<Double, String>> testList = new ArrayList<Tuple2<Double, String>>();
        testList.add(new Tuple2<Double, String>(1.0, "spark f g h"));
        testList.add(new Tuple2<Double, String>(1.0, "hadoop mapreduce"));
        //rdd1
        JavaRDD<Tuple2<Double, String>> testrdd1 = sc.parallelize(testList);
        //rdd2
        JavaRDD<Row> testrdd2 = testrdd1.map(new Function<Tuple2<Double, String>, Row>() {
            public Row call(Tuple2<Double, String> v1) throws Exception {
                return RowFactory.create(v1._1, v1._2);
            }
        });

        Dataset<Row> testdf1 = spark.createDataFrame(testrdd2, type);
        Dataset<Row> result = model.transform(testdf1) ;
        List<Row> list2 = result.collectAsList();
        for(Row row : list2){
            SparseVector sv = ((GenericRowWithSchema)row).<SparseVector>getAs("features") ;
            //转换成密集向量
            DenseVector dv = sv.toDense();
            System.out.println(dv);
        }
    }
}
