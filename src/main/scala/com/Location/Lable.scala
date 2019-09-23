package com.Location

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object Lable {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath, outputPath) = args
    val spark = SparkSession.builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df = spark.read.parquet(inputPath)
    df.rdd.map(row => {
      val adspacetype = row.getAs[Int]("adspacetype")
      var key=""
      if(adspacetype<10){
        key="LN0"+adspacetype.toString
      }else{
        key="LN"+adspacetype.toString
      }
      (key,1)
    }).reduceByKey(_+_)
      .map(x => x._1 + "," + x._2).saveAsTextFile(outputPath)
    spark.stop()
  }
}
