package group_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("one").master("local").getOrCreate()

    val sc = spark.sparkContext

    val rdd1 = sc.makeRDD(Seq((1,"a"),(2,null),(3,null),(4,null),(5,null)))
    val rdd2 = sc.makeRDD(Seq((1,1),(2,1),(3,1),(4,1),(5,1)))
    rdd2.join(rdd1)
    sc.stop()
    spark.stop()
  }
}
