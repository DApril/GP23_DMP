package group_Test

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * 图计算案例（好友关联推荐）
  */
object GraphTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("graph").master("local").getOrCreate()

    //创建点和边
    //构建点的集合
    val vertexRDD = spark.sparkContext.makeRDD(Seq(
      (1L, ("小1", 26)),
      (2L, ("小2", 30)),
      (6L, ("小3", 22)),
      (9L, ("小4", 22)),
      (133L, ("小5", 26)),
      (138L, ("小6", 30)),
      (158L, ("小7", 22)),
      (16L, ("小8", 30)),
      (44L, ("小9", 22)),
      (21L, ("小10", 22)),
      (5L, ("小11", 22)),
      (7L, ("小12", 22))
    ))
    val edgeRDD = spark.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(7L, 158L, 0),
      Edge(5L, 158L, 0)
    ))
    //构造边的集合

    //构建图
    val graph = Graph(vertexRDD,edgeRDD)
    //取顶点
   graph.connectedComponents().vertices.foreach(println)

    //匹配数据
//    vertices.join(vertexRDD).map{
//      case (userId,(cnId,(name,age)))=>(cnId,List((name,age)))
//    }.reduceByKey(_++_).foreach(println)

    spark.stop()
  }
}
