package net.sansa_stack.ml.spark.kge.linkprediction.NewDesign

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.util.Random
import org.apache.spark.sql.functions._

case class test(s: String, i: Int) {
  override def toString(): String = {
    val r = "<< s= " + s + " , i= " + i.toString() + " >>"
    return r
  }
}

case class test2(s: String, i: Int)

object tempTest extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("TransE")
    .getOrCreate()
  import spark.implicits._

  println("--- TEST ----")

//  println(test("hello", 123).toString())
//
//  val x: Array[test2] = List(test2("s1", 11), test2("s2", 12)).toArray
//  x.foreach(println)
//
//  val x2: Array[test] = List(test("s1", 11), test("s2", 12)).toArray
//  x2.foreach(t => println(t.toString()))

  case class Feature(lemma: String, pos_tag: String, ne_tag: String)
  case class Record(id: Long, content_processed: Seq[Feature])

  val df =
    Seq(
      Record(1L, Seq(
        Feature("ancient", "jj", "o"),
        Feature("olympia_greece", "nn", "location")))).toDF();
  
  df.foreach(println(_))

  val content = df.select($"content_processed").rdd.map(_.getSeq[Row](0))

  content.map(_.map {
    case Row(lemma: String, pos_tag: String, ne_tag: String) =>
      (lemma, pos_tag, ne_tag)
  })

  content.map(_.map(row => (
    row.getAs[String]("lemma"),
    row.getAs[String]("pos_tag"),
    row.getAs[String]("ne_tag"))))

  df.as[Record].rdd.map(_.content_processed)

  df.select($"content_processed").as[Seq[(String, String, String)]]
  
  
}