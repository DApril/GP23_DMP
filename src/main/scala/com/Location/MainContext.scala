package com.Location

import com.util.JsonUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object MainContext {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("core")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("D://abc//json.txt")
        lines.map(jsonstr=>{
          val arr = JsonUtils.getBusinessarea(jsonstr).split(",")
          (arr(0),arr.size)
        }).filter(x=>x._1!="[]").foreach(println)
        val rdd1: RDD[String] = lines.map(jsonstr => {
          JsonUtils.getType(jsonstr)
        })
       rdd1.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).foreach(println)
    sc.stop()
  }
}
