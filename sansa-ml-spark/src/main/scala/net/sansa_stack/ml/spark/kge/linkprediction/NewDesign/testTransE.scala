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
import org.apache.commons.math3.analysis.function.Pow







object testTransE extends App {
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("TransE")
    .getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  println("<<< STARTING >>>")
  
  val trp = new Triples("train","/home/hamed/workspace/TransE/DataSets/FB15k/freebase_mtr100_mte100-train.txt",spark)
  

  println("\n\n No triples = ",  trp.triples.count() )
  
  trp.getAllDistinctEntities().take(10).toArray.foreach(println)
  println("---------")
  trp.getSortedAllDistinctEntities().take(10).toArray.foreach(println)
  
  println("\n\n--- ByIndexConverter ---")
  val conv = new ByIndexConverter(trp,spark)
  
  val ent = MyUtility.time{ conv.getEntities() }
  val pred = MyUtility.time{ conv.getPredicates() }
  
  val train = MyUtility.time{ conv.getConvertedTriples() }
  
  MyUtility.time{
  println("train=",train.count() ) 
  }
  
  
//  train.sample(false, 0.001).rdd.collect().foreach(println)
  
  val convertedback = MyUtility.time{
    conv.getTriplesByString(train)
  }
  
//  var transE = new TransE(spark,
//      
//              train : Dataset[RecordLongTriples],
//              entities : Dataset[Long],
//              relations : Dataset[Long],
//              margin : Float,
//              embeddingDimention : Int,
//              batchSize : Int,
//              epochSize : Int,
//              learningRate : Float)

  
  
  
  println("\n\n<<< DONE >>>")
  
}