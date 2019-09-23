package com.Tag

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsAd extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row:Row = args(0).asInstanceOf[Row]
    //获取广告位类型
    val adType = row.getAs[Int]("adspacetype")
    //获取广告位名称
    val adName = row.getAs[String]("adspacetypename")
    adType match {
      case v if v>9 =>list:+=("LC"+v,1)
      case v if v>0 && v<=9 => list:+=("LC0"+v,1)
    }
    if(StringUtils.isNotBlank(adName)){
      list:+=("LN"+adName,1)
    }
    list
  }
}
