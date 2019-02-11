package com.spark.c9_hiddenModel

// Define basic library
import org.apache.spark.sql.Row

// Define library Related to RDD Registry
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType

import org.apache.spark.sql.SparkSession

// Functions for week Calculation
import java.util.Calendar
import java.text.SimpleDateFormat

object s3_predictPart2 {
  def main(args: Array[String]): Unit = {


    // User Defined Function
    // Purpose of Funtion : Generate forecast week
    def fcstWeekGenerate(rawData:  Iterable[(String, String, String, Double, Double)], pastWeek: String, fcstWeek: Int): Iterable[(String, String, String, Double, Double)] = {
      //var rawData = data
      var resultBuffer = rawData.toArray

      var iterWeek = rawData.size + fcstWeek
      var resultArray = Array.fill(iterWeek)("salesid", "item","yearweek", 0.0d, 0.0d)

      var i = 0
      var salesid = resultBuffer.head._1
      var item = resultBuffer.head._2
      var average4week = resultBuffer.head._5
      var standardQty = Math.round(resultBuffer.head._5)

      var yearWeek = pastWeek

      while( i < iterWeek) {
        if( i < resultBuffer.size) {
          resultArray(i) = (resultBuffer(i)._1, resultBuffer(i)._2, resultBuffer(i)._3, resultBuffer(i)._4, resultBuffer(i)._5)
        } else {
          yearWeek = postWeek(yearWeek, 1)
          resultArray(i) = (salesid, item, yearWeek, standardQty, average4week)
        } // end of if

        i = i+1
      } // end of while

      var genData = resultArray.toIterable
      genData
    } // end of function

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

    //////////////////////////////////////////////////////////////////////////////////////////////////
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
    var selloutDb = "kopo_predict_middleresult1"

    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle.createOrReplaceTempView("middleresult")

    println(selloutDataFromOracle.show())
    println("oracle ok")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. data refining
    //////////////////////////////////////////////////////////////////////////////////////////////////
    //    var selloutFilteredDf = spark.sql("select salesid, item, yearweek," +
    //      "cast(qty as double) as qty, " +
    //      "cast(average4week as double) as average4week, " +
    //      "ap2id, productgroup, product, planweek from middleresult where 1=1")
    //var selloutFilteredDf = sqlContext.sql("select salesid, item, yearweek, qty, 50.2 as average4week from brt_channel_result where productgroup = 'WM'")

    var selloutFilteredDf = spark.sql("""
      select salesid, item, yearweek,
      cast(qty as double) as qty,
      cast(average4week as double) as average4week,
      ap2id, productgroup, product, planweek from middleresult where 1=1""")
    //var selloutFilteredDf = sqlContext.sql("select salesid, item, yearweek, qty, 50.2 as average4week from brt_channel_result where productgroup = 'WM'")


    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 3. data column indexing & make rdd
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var selloutColumnIndex = selloutFilteredDf.columns.map(x=>{x.toLowerCase()})

    var salesidIndex = selloutColumnIndex.indexOf("salesid")
    var itemIndex = selloutColumnIndex.indexOf("item")
    var yearweekIndex = selloutColumnIndex.indexOf("yearweek")
    var qtyIndex = selloutColumnIndex.indexOf("qty")
    var average4weekIndex = selloutColumnIndex.indexOf("average4week")
    var ap2idIndex = selloutColumnIndex.indexOf("ap2id")
    var productgroupIndex = selloutColumnIndex.indexOf("productgroup")
    var productIndex = selloutColumnIndex.indexOf("product")
    var planweekIndex = selloutColumnIndex.indexOf("planweek")

    var selloutRdd = selloutFilteredDf.rdd

    var fcstWeekGenRdd = selloutRdd.
      groupBy(x=>{
        (x.getString(salesidIndex), x.getString(itemIndex)) }).
      flatMap(x=>{
        var key = x._1
        var data = x._2

        var ap2id = data.map(x=>{x.getString(ap2idIndex)}).head
        var productgroup = data.map(x=>{x.getString(productgroupIndex)}).head
        var product = data.map(x=>{x.getString(productIndex)}).head
        var currWeek = data.map(x=>{x.getString(planweekIndex)}).head

        currWeek = postWeek(currWeek,1)

        var pastSales = data.map(x=>{
          ( x.getString(salesidIndex), x.getString(itemIndex), x.getString(yearweekIndex), x.getDouble(qtyIndex), x.getDouble(average4weekIndex) ) })

        var getFcstData = fcstWeekGenerate(pastSales, preWeek(currWeek,1), 26)

        var fcstResult = getFcstData.filter(
          x=>{ true
            //x._3.toInt >= currWeek.toInt
          }).map(x => {
          Row.fromSeq(Seq( x._1, x._2, x._3, x._4, x._5, ap2id, productgroup, product, currWeek ))
          // (salesid, item, yearweek, qty, 4weekAverage, ap2id, productgroup, product)
        })

        fcstResult
      })

    var fcstResultDf = spark.createDataFrame(fcstWeekGenRdd,
      StructType(Seq(
        StructField("SALESID", StringType),
        StructField("ITEM", StringType),
        StructField("YEARWEEK", StringType),
        StructField("QTY", DoubleType),
        StructField("AVERAGE4WEEK", DoubleType),
        StructField("AP2ID", StringType),
        StructField("PRODUCTGROUP", StringType),
        StructField("PRODUCT", StringType),
        StructField("PLANWEEK", StringType)
      )))
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    val table = "kopo_predict_middleresult2"

    fcstResultDf.show(3)
    fcstResultDf.write.mode("overwrite").jdbc(staticUrl, table, prop)
  }
}
