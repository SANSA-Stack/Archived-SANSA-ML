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


import net.sansa_stack.ml.spark.kge.linkprediction.NewDesign._


object runTesting extends App {
  

  def printType[T](x: T): Unit = { println(x.getClass.toString()) }

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("TransE")
    .getOrCreate()
    
  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  println("<<< STARTING >>>")
  
  val trp = new Triples("train","/home/hamed/workspace/TransE/DataSets/FB15k/freebase_mtr100_mte100-test.txt",spark)
  
//  println(f"\rNo triples = ${trp.triples.count()}" )
//  trp.getAllDistinctEntities().take(10).foreach(println)
//  println(f"\rNo entities = ${trp.getAllDistinctEntities().count()}" )
//  println(f"\rNo predicates = ${trp.getAllDistinctPredicates().count()}" )
//  
//  val e1 = trp.getAllDistinctEntities().take(10).toSeq.toDS()
//  println(f"\n----------")
//  e1.foreach(x=>println(f"${x}"))
  
  val n = 10
  val conv = new ByIndexConverter(trp,spark)
  
//  val id1 = conv.entities.select("ID").sample(false,0.2).take(n)
//  val ind1 = id1.map( row => row(0).asInstanceOf[Long]).toSeq.toDS()
//  
//  val r1 = conv.getEntitiesByIndex(ind1).persist()
//  println(f"\ncount = ${r1.count}")
//  r1.show()
//
//  val id2 = conv.predicates.select("ID").sample(false, 0.2).take(n)
//  val ind2 = id2.map( row => row(0).asInstanceOf[Long]).toSeq.toDS()
//  
//  val r2 = conv.getPredicatesByIndex(ind2).persist()
//  println(f"\ncount = ${r2.count}")
//  r2.show()
//
//  val e = conv.getEntities().as[Long].collect
// 
//  println(f"\ne.length=${e.length} , e.min = ${e.min} , e.max = ${e.max}")
  
//  for(i <- 1 to 3){
//     conv.getEntities().sample(false,.01).show()
//  }
//  System.exit(0)
  
 
  println("\n\n------ TESTING -----")
  
  lazy val smp1 = trp.triples.take(n)
  val sample1 = smp1.toSeq.toDS()
  println(sample1.rdd.collect().head )
  
  sample1.show()
//  MyUtility.exit

  
  val r3 = conv.getTriplesByIndex(sample1)
  r3.printSchema()
//  printType(r3)

  r3.show()
  
  


//  val t = r3.map{ case r:RecordLongTriples => r.Predicate.toString()}.collect()
//  t.foreach(println)
  
//  val y =r3.map{ case r: RecordLongTriples => (r.Subject,r.Predicate,r.Object) }
//     
//  y.foreach{ t => println(t.toString())}
  
  
  val r4 = conv.getTriplesByString(r3)
  
  r4.show()
  println("<<< DONE >>>")
}