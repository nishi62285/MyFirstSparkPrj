package com.my.training

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._
object MySparkPrj{
  def main(args:Array[String] ):Unit= {
  //val a = new SparkConf().setAppName("Demo").setMaster("local").set("spark.testing.memory","471859200")
  val a = new SparkConf().setAppName("MySparkPrj").setMaster("local").set("spark.testing.memory","471859200")
  val sc = new SparkContext(a)
  val b = sc.textFile("file:///home/s1/Desktop/Data/testdata.txt")
  val c = b.flatMap(x=>x.split(" "))
  val d = c.map(x=>(x,1))
  d.reduceByKey(_+_).collect()
}
}
