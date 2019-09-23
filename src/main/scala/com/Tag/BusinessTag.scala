package com.Tag

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisConnectionPool, String2Type, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 商圈标志
  */
object BusinessTag extends Tag{

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据
    val row = args(0).asInstanceOf[Row]
    //获取经纬度
    val long = String2Type.toDouble(row.getAs[String]("long"))
    val lat = String2Type.toDouble(row.getAs[String]("lat"))
    if(long>=73 && long< 136 && lat>=3 && lat<=53){
       //获取到商圈名称
      val business = getBusiness(long,lat)
      if(StringUtils.isNoneBlank(business)){
        val str = business.split(",")
        str.foreach(str=>{
          list:+=(str,1)
        })
      }
    }
    list
  }




  /**
    *获取商圈信息
    */
  def getBusiness(long: Double, lat: Double):String = {
    //1.先去redis查询数据库中是否有商圈信息
    //GeoHash码
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)
    //数据库查询当前商圈信息
    var business = redis_queryBussiness(geohash)
    if(business ==null){
      business = AmapUtil.getBusinessFromAmap(long,lat)
      //将高德获取的商圈存储在redis数据库
      if(business!=null && business.length>0){
        redis_insertBusiness(geohash,business)
      }
    }
    business
  }
  //去redis查询商圈信息
  def redis_queryBussiness(geohash: String): String = {
    val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }
  //去redis存储商圈信息
  def redis_insertBusiness(geohash: String, business: String) = {
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }
}
