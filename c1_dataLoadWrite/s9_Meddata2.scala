package com.spark.c1_dataLoadWrite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s9_Meddata2 {
  def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("DataFrame")//.setMaster("local[4]")
  var sc = new SparkContext(conf)
  val spark = new SQLContext(sc)
  import spark.implicits._

  ///////////////////////////  데이터 파일 로딩  ////////////////////////////////////
  // 접속정보 설정 (1)
  var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var paramTable = "kopo_parameter"

  // 관계형 데이터베이스 Oracle 연결 (2)
  val paramData= spark.read.format("jdbc").
    option("url",staticUrl).
    option("dbtable",paramTable).
    option("user",staticUser).
    option("password",staticPw).load

  // 컬럼별 인데스 생성
  var paramColumns = paramData.columns
  var pcatNo = paramColumns.indexOf("PARAM_CATEGORY")
  var pnameNo = paramColumns.indexOf("PARAM_NAME")
  var valueNo = paramColumns.indexOf("PARAM_VALUE")

  var paramRdd = paramData.rdd
  var paramGroupMap = paramRdd.groupBy(x=>{
    (x.getString(pcatNo),
      x.getString(pnameNo)) }).
    map(x=>{
      // 그룹별 분산처리가 수행됨
      var key = x._1
      var data = x._2
      var paramValue = data.map(x=>{x.getString(valueNo)}).toArray
      (key, paramValue)
    }).collectAsMap

  var inTable = paramGroupMap("COMMON","INTABLE")(0)
  var outTable = paramGroupMap("COMMON","OUTTABLE")(0)

  // 관계형 데이터베이스 Oracle 연결 (2)
  val inData= spark.read.format("jdbc").
    option("url",staticUrl).
    option("dbtable",inTable).
    option("user",staticUser).
    option("password",staticPw).load

  var hdfs_master = "hdfs://192.168.110.117:9000/kopo/"

  inData.
    coalesce(1). // 파일개수
    write.format("csv").  // 저장포맷
    mode("overwrite"). // 저장모드 append/overwrite
    option("header", "true"). // 헤더 유/무
    save(hdfs_master+outTable) // 저장파일명

  }
}
