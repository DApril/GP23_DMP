package com.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils

object JsonUtils {
     def getBusinessarea(jsonstr:String):String ={
       val jSONObject1 = JSON.parseObject(jsonstr)
       // 判断当前状态是否为 1
       val status = jSONObject1.getIntValue("status")
       if(status == 0) return ""
       ""
       val jSONObject2 = jSONObject1.getJSONObject("regeocode")
       if(jSONObject2==null) return ""
       val jSONArray = jSONObject2.getJSONArray("pois")
       if(jSONArray == null) return  ""
       val result = collection.mutable.ListBuffer[String]()
       for (item <- jSONArray.toArray()){
         if(item.isInstanceOf[JSONObject]){
           val json = item.asInstanceOf[JSONObject]
           val businessarea = json.getString("businessarea")
             result.append(businessarea)
         }
       }
       result.mkString(",")
     }
  def getType(jsonstr:String):String ={
    val jSONObject1 = JSON.parseObject(jsonstr)
    // 判断当前状态是否为 1
    val status = jSONObject1.getIntValue("status")
    if(status == 0) return ""
    ""
    val jSONObject2 = jSONObject1.getJSONObject("regeocode")
    if(jSONObject2==null) return ""
    val jSONArray = jSONObject2.getJSONArray("pois")
    if(jSONArray == null) return  ""
    val result = collection.mutable.ListBuffer[String]()
    for (item <- jSONArray.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        val type1 = json.getString("type")
        val strs = type1.split(";|\\|")
        for(str<-strs){
          result.append(str)
        }
      }

    }
    result.mkString(",")
  }
}
