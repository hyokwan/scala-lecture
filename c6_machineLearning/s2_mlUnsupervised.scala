package com.spark.c6_machineLearning

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans

object s2_mlUnsupervised {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /// 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var filePath = "c:/spark/bin/data/"
    var sampleName = "student_middle.csv"

    // 파일 불러오기 함수 정의
    def csvFileLoading(workPath: String, fileName: String)
    : org.apache.spark.sql.DataFrame = {
      var outDataFrame=
        spark.read.format("csv").
          option("header","true").
          option("Delimiter",",").
          load(workPath+fileName)
      outDataFrame
    }
    var sampleData = csvFileLoading(filePath,sampleName)

    // 데이터 타입 변경
    var kmeansInput = sampleData.
      withColumn("SW", $"SW".cast("Double")).
      withColumn("DB", $"DB".cast("Double")).
      withColumn("AND", $"AND".cast("Double"))

    kmeansInput.dtypes.foreach(println)

    // 훈련
    var assembler = new VectorAssembler().
      setInputCols(Array("SW","DB","AND")).
      setOutputCol("FEATURES")

    var kValue = 2
    val kmeans = new KMeans().setK(kValue).
      setFeaturesCol("FEATURES").
      setPredictionCol("PREDICTION")
    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))
    val kMeansPredictionModel = pipeline.fit(kmeansInput)

    //예측
    val predictionResult = kMeansPredictionModel.transform(kmeansInput)

    print(predictionResult.show(10))
  }
}
