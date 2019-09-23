package com.Location

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 统计省市指标
  */
object ProCityCt {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    if(args.length != 1){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath) =args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 获取数据
    val df = spark.read.parquet(inputPath)
    // 注册临时视图
    df.createTempView("log")

    //地域分布
//    val sql ="select " +
//      "provincename," +
//      "cityname," +
//      "sum(one)," +
//      "sum(two)," +
//      "sum(three)," +
//      "sum(four)," +
//      "sum(five)," +
//      "1.0*sum(five)/sum(four)," +
//      "sum(six)," +
//      "sum(seven)," +
//      "1.0*sum(seven)/count(seven)," +
//      "sum(nine)," +
//      "sum(eight) " +
//      "from" +
//      "("+
//      "select " +
//      "provincename," +
//      "cityname," +
//      "(case when requestmode=1 and processnode>=1 then 1 else 0 end) one," +
//      "(case when requestmode=1 and processnode>=2 then 1 else 0 end) two," +
//      "(case when requestmode=1 and processnode=3 then 1 else 0 end) three," +
//      "(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) four," +
//      "(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) five," +
//      "(case when requestmode=2 and iseffective=1 then 1 else 0 end) six," +
//      "(case when requestmode=3 and iseffective=1 then 1 else 0 end) seven," +
//      "(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) eight," +
//      "(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) nine " +
//      "from log"+
//      ") tmp " +
//      "group by provincename,cityname "+
//      "order by provincename,cityname"
    //终端设备
//    val sql ="select " +
//      "ispname," +
//      "sum(one)," +
//      "sum(two)," +
//      "sum(three)," +
//      "sum(four)," +
//      "sum(five)," +
//      "1.0*sum(five)/sum(four)," +
//      "sum(six)," +
//      "sum(seven)," +
//      "1.0*sum(seven)/count(seven)," +
//      "sum(nine)," +
//      "sum(eight) " +
//      "from" +
//      "("+
//      "select " +
//      "ispname," +
//      "(case when requestmode=1 and processnode>=1 then 1 else 0 end) one," +
//      "(case when requestmode=1 and processnode>=2 then 1 else 0 end) two," +
//      "(case when requestmode=1 and processnode=3 then 1 else 0 end) three," +
//      "(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) four," +
//      "(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) five," +
//      "(case when requestmode=2 and iseffective=1 then 1 else 0 end) six," +
//      "(case when requestmode=3 and iseffective=1 then 1 else 0 end) seven," +
//      "(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) eight," +
//      "(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) nine " +
//      "from log"+
//      ") tmp " +
//      "group by ispname "+
//      "order by ispname"
    val sql ="select " +
      "appname," +
      "sum(one)," +
      "sum(two)," +
      "sum(three)," +
      "sum(four)," +
      "sum(five)," +
      "1.0*sum(five)/sum(four)," +
      "sum(six)," +
      "sum(seven)," +
      "1.0*sum(seven)/count(seven)," +
      "sum(nine)," +
      "sum(eight) " +
      "from" +
      "("+
      "select " +
      "appname," +
      "(case when requestmode=1 and processnode>=1 then 1 else 0 end) one," +
      "(case when requestmode=1 and processnode>=2 then 1 else 0 end) two," +
      "(case when requestmode=1 and processnode=3 then 1 else 0 end) three," +
      "(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) four," +
      "(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) five," +
      "(case when requestmode=2 and iseffective=1 then 1 else 0 end) six," +
      "(case when requestmode=3 and iseffective=1 then 1 else 0 end) seven," +
      "(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) eight," +
      "(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) nine " +
      "from log"+
      ") tmp " +
      "group by appname "+
      "order by appname"
    val df2 = spark.sql(sql)
    df2.show()
   // df2.write.partitionBy("provincename","cityname").json("D:\\procity")
    // 存Mysql

    // 通过config配置文件依赖进行加载相关的配置信息
//        val load = ConfigFactory.load()
//        // 创建Properties对象
//        val prop = new Properties()
//        prop.setProperty("user",load.getString("jdbc.user"))
//        prop.setProperty("password",load.getString("jdbc.password"))
//        // 存储
//        df2.write.mode(SaveMode.Append).jdbc(
//          load.getString("jdbc.url"),load.getString("jdbc.tablName"),prop)

    spark.stop()
  }
}
