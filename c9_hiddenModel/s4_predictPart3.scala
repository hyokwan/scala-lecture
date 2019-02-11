package com.spark.c9_hiddenModel

import org.apache.spark.sql.SparkSession

// Define basic library
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.Row

// Define library Related to RDD Registry
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType

// Functions for week Calculation
import java.util.Calendar
import java.text.SimpleDateFormat

object s4_predictPart3 {
  def main(args: Array[String]): Unit = {


    // User Defined Function
    // Purpose of Funtion : Calculate pre week
    def preWeek(inputYearWeek: String, gapWeek: Int): String = {
      val currYear = inputYearWeek.substring(0, 4).toInt
      val currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance();
      calendar.setMinimalDaysInFirstWeek(4);
      calendar.setFirstDayOfWeek(Calendar.MONDAY);

      var dateFormat = new SimpleDateFormat("yyyyMMdd");

      calendar.setTime(dateFormat.parse(currYear + "1231"));
      //    calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (currWeek <= gapWeek) {
        var iterGap = gapWeek - currWeek
        var iterYear = currYear - 1

        calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"));
        var iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

        while (iterGap > 0) {
          if (iterWeek <= iterGap) {
            iterGap = iterGap - iterWeek
            iterYear = iterYear - 1
            calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))
            iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
          } else {
            iterWeek = iterWeek - iterGap
            iterGap = 0
          } // end of if
        } // end of while

        return iterYear.toString + "%02d".format(iterWeek)
      } else {
        var resultYear = currYear
        var resultWeek = currWeek - gapWeek

        return resultYear.toString + "%02d".format(resultWeek)
      } // end of if
    } // end of function

    // User Defined Function
    // Purpose of Funtion : Calculate post week
    def postWeek(inputYearWeek: String, gapWeek: Int): String = {
      val currYear = inputYearWeek.substring(0, 4).toInt
      val currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance();
      calendar.setMinimalDaysInFirstWeek(4);
      calendar.setFirstDayOfWeek(Calendar.MONDAY);

      var dateFormat = new SimpleDateFormat("yyyyMMdd");

      calendar.setTime(dateFormat.parse(currYear + "1231"));

      var maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (maxWeek < currWeek + gapWeek) {
        var iterGap = gapWeek + currWeek - maxWeek
        var iterYear = currYear + 1

        calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"));
        var iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

        while (iterGap > 0) {
          if (iterWeek < iterGap) {
            iterGap = iterGap - iterWeek
            iterYear = iterYear + 1
            calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))
            iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
          } else {
            iterWeek = iterGap
            iterGap = 0
          } // end of if
        } // end of while

        return iterYear.toString() + "%02d".format(iterWeek)
      } else {
        return currYear.toString() + "%02d".format((currWeek + gapWeek))
      } // end of if
    } // end of function
    //val seasonalDf = sqlContext.read.format("jdbc").options(
    //Map("url" -> "jdbc:postgresql://127.0.0.1:5432/postgres","dbtable" ->
    //"brt_batch_season_result", "user" -> "postgres", "password" -> "brightics")).load
    //seasonalDf.registerTempTable("seasonTable")

    // 1. data loading
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 1. data loading
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    //staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_batch_season_result"

    val seasonDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    seasonDataFromOracle.createOrReplaceTempView("seasonresult")

    println(seasonDataFromOracle.show())
    println("oracle ok")

    seasonDataFromOracle.createOrReplaceTempView("seasonTable")

    var seasonDf = spark.sql("select product, week, cast(avg_ratio as double) as seasonality, regionid   from seasonTable where 1=1")
    var seasonColumnIndex = seasonDf.columns.map(x=>{x.toLowerCase()})

    var seasonAp2idIndex = seasonColumnIndex.indexOf("regionid")
    var seasonProductIndex = seasonColumnIndex.indexOf("product")
    var seasonWeekIndex = seasonColumnIndex.indexOf("week")
    var seasonSeasonalityIndex = seasonColumnIndex.indexOf("seasonality")

    var seasonMap = seasonDf.rdd.groupBy(x=>{
      ( x.getString(seasonAp2idIndex), x.getString(seasonProductIndex), x.getString(seasonWeekIndex) )}).
      map(x=>{
        var key = x._1
        var data = x._2
        var seasonaltiy = data.map(x=>{ x.getDouble(seasonSeasonalityIndex) }).head
        (key,seasonaltiy)
      }).collectAsMap

    //val fcstDf = sqlContext.read.format("jdbc").options(
    //Map("url" -> "jdbc:postgresql://127.0.0.1:5432/postgres","dbtable" ->
    //"middle_result", "user" -> "postgres", "password" -> "brightics")).load
    //fcstDf.registerTempTable("fcstTable")

    var middleResult2Db = "kopo_predict_middleresult2"

    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> middleResult2Db, "user" -> staticUser, "password" -> staticPw)).load

    println(selloutDataFromOracle.show(2))
    println("oracle ok")
    println("oracle middle result 2 ok")

    selloutDataFromOracle.createOrReplaceTempView("fcstTable")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. data refining
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var fcstFilteredDf = spark.sql("select salesid, item, yearweek, cast(qty as double) as qty, cast(average4week as double), ap2id, productgroup, product, planweek from fcstTable where 1=1")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 3. data column indexing & make rdd
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var fcstColumnIndex = fcstFilteredDf.columns.map(x=>{x.toLowerCase()})

    var salesidIndex = fcstColumnIndex.indexOf("salesid")
    var itemIndex = fcstColumnIndex.indexOf("item")
    var yearweekIndex = fcstColumnIndex.indexOf("yearweek")
    var qtyIndex = fcstColumnIndex.indexOf("qty")
    var averageIndex = fcstColumnIndex.indexOf("average4week")
    var ap2idIndex = fcstColumnIndex.indexOf("ap2id")
    var productgroupIndex = fcstColumnIndex.indexOf("productgroup")
    var productIndex = fcstColumnIndex.indexOf("product")
    var planweekIndex = fcstColumnIndex.indexOf("planweek")

    //5002479 DVG45M5500P/A3 201742, 3.25
    var fcstRdd = fcstFilteredDf.rdd.
      groupBy(x=>{
        (x.getString(salesidIndex), x.getString(itemIndex)) }).
      flatMap(x=>{
        //var x = fcstRdd2.filter(x=>{x._1._1=="5002479"&x._1._2=="DVG45M5500P/A3"}).first
        var key = x._1
        var data = x._2

        var ap2id = data.map(x=>{x.getString(ap2idIndex)}).head
        var productgroup = data.map(x=>{x.getString(productgroupIndex)}).head
        var product = data.map(x=>{x.getString(productIndex)}).head
        var currWeek = data.map(x=>{x.getString(planweekIndex)}).head

        var pre1week = preWeek(currWeek,1).substring(4)
        var pre2week = preWeek(currWeek,2).substring(4)
        var pre3week = preWeek(currWeek,3).substring(4)
        var pre4week = preWeek(currWeek,4).substring(4)

        var pastAverageSeasonlity = 0.0d
        if( seasonMap.contains(ap2id, product, pre1week)&&
          seasonMap.contains(ap2id, product, pre2week)&&
          seasonMap.contains(ap2id, product, pre3week)&&
          seasonMap.contains(ap2id, product, pre4week)){
          pastAverageSeasonlity =
            (seasonMap(ap2id, product, pre1week)+
              seasonMap(ap2id, product, pre2week)+
              seasonMap(ap2id, product, pre3week)+
              seasonMap(ap2id, product, pre4week)) / 4
        }

        var fcst = data.map(x=>{
          var average = x.getDouble(averageIndex)
          var week = x.getString(yearweekIndex).substring(4)

          var seasonality = 0.0d
          if(seasonMap.contains(ap2id, product, week)){
            seasonality = seasonMap(ap2id, product, week).toDouble
          }

          var fcstValue = if(x.getString(yearweekIndex).toInt < currWeek.toInt){
            x.getDouble(qtyIndex)
          }else{
            average * (1+seasonality) / (1+pastAverageSeasonlity)
          }

          Row(
            x.getString(salesidIndex)+"_"+x.getString(itemIndex),
            x.getString(salesidIndex),
            x.getString(itemIndex),
            x.getString(yearweekIndex),
            x.getString(yearweekIndex).substring(4,6),
            x.getDouble(averageIndex),
            x.getDouble(qtyIndex),
            Math.round(fcstValue).toDouble,
            ap2id,
            productgroup,
            product,
            currWeek) })

        ////////////////////////Insert FCST Logic ///////////////////////////////////////
        //fcstResult
        fcst
      })

    var fcstResultDf = spark.createDataFrame(fcstRdd,
      StructType(Seq(
        StructField("IDX", StringType),
        StructField("SALESID", StringType),
        StructField("ITEM", StringType),
        StructField("YEARWEEK", StringType),
        StructField("WEEK", StringType),
        StructField("AVERAGE4WEEK", DoubleType),
        StructField("INFCST", DoubleType),
        StructField("OUTFCST", DoubleType),
        StructField("REGIONID", StringType),
        StructField("PRODUCTGROUP", StringType),
        StructField("PRODUCT", StringType),
        StructField("PLANWEEK", StringType)
      )))

    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    val table = "kopo_predict_middleresult3"

    fcstResultDf.write.mode("overwrite").jdbc(staticUrl, table, prop)
    fcstResultDf.show(3)
  }
}
