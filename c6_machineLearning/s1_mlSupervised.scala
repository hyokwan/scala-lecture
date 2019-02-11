package com.spark.c6_machineLearning


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

// 머신러닝 라이브러리(ml) 로딩
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.
        {DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.ml.feature.{StringIndexer}

object s1_mlSupervised {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /// 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var filePath = "c:/spark/bin/data/"
    var sampleName = "kopo_decisiontree_input.csv"

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
    print(sampleData.show(5))

    // 데이터 타입 변경
    var dtInput = sampleData.
      withColumn("QTY", $"QTY".cast("Double")).
      withColumn("PRO_PERCENT", $"PRO_PERCENT".cast("Double"))

    dtInput.dtypes.foreach(println)

    // 인덱서 및 특성 컬럼 생성
    val holidayIndexer = new StringIndexer().
      setInputCol("HOLIDAY").setOutputCol("HOLIDAY_IN")
    val promotionIndexer = new StringIndexer().
      setInputCol("PROMOTION").setOutputCol("PROMOTION_IN")

    // 과거(학습) / 미래(예측) 데이터 분리
    var targetYearweek = 201630
    val trainData = dtInput.filter($"YEARWEEK" <= targetYearweek)
    val testData = dtInput.filter($"YEARWEEK" > targetYearweek)

    // 훈련 (TrainingData)
    val assembler = new VectorAssembler().
     setInputCols(Array("HOLIDAY_IN","PROMOTION_IN","PRO_PERCENT")).
     setOutputCol("FEATURES")

    var dt = new DecisionTreeRegressor().
      setLabelCol("QTY").
      setFeaturesCol("FEATURES")

    val pipeline = new Pipeline().
      setStages(Array(holidayIndexer, promotionIndexer, assembler, dt))

    val model = pipeline.fit(trainData)

    // 예측 (TestData)
    val predictions = model.transform(testData)

    predictions.select("REGIONID","PRODUCT","ITEM","YEARWEEK","QTY","FEATURES","PREDICTION").
      orderBy("YEARWEEK").show

    // 예측모델 평가
    val evaluatorRmse = new RegressionEvaluator().
      setLabelCol("QTY").
      setPredictionCol("prediction").
      setMetricName("rmse")
    val evaluatorMae = new RegressionEvaluator().
      setLabelCol("QTY").
      setPredictionCol("prediction").
      setMetricName("mae")
    val rmse = evaluatorRmse.evaluate(predictions)
    val mae = evaluatorMae.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
    println("Mean Average Error (MAE) on test data = " + mae)

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
    println("Learned regression tree model:\n" + treeModel.toDebugString)

  }
}
