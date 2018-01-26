package net.sansa_stack.ml.spark.kge.linkprediction.NewDesign

import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import ml.dmlc.mxnet._



object playingWithMXNet extends App {
 
	Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("TransE")
    .getOrCreate()
    
//import org.apache.mxnet.NDArray

/**
 * A wrapper for serialize & deserialize [[org.apache.mxnet.NDArray]] in spark job
 * @author Yizhi Liu
 */
class MXNDArray(@transient private var ndArray: NDArray) extends Serializable {
  require(ndArray != null)
  private val arrayBytes: Array[Byte] = ndArray.serialize()

  def get: NDArray = {
    if (ndArray == null) {
      ndArray = NDArray.deserialize(arrayBytes)
    }
    ndArray
  }
}

object MXNDArray {
  def apply(ndArray: NDArray): MXNDArray = new MXNDArray(ndArray)
}
    
  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  println("<<< STARTING >>>")
  
  implicit val encoder = org.apache.spark.sql.Encoders.javaSerialization(classOf[MXNDArray])
  
  
  
  val ndarr = MXNDArray(NDArray.ones(Shape(2,3)))
  
  
  
  println("ndarr =")
  ndarr.get.toArray.foreach(println(_))
  
//  ndarr.toArray.foreach(println(_))
  
//  val x = ndarr.copy() * 2
//  x.toArray.foreach(println(_))
//  ndarr.toArray.foreach(println(_))
//  
//  val ndarrList = (
//      (1,ndarr) :: (2, ndarr.copy() * 2f ) :: (3,ndarr.copy() * 3f) :: Nil //(2,ndarr.copy()*2.0)::(3,ndarr.copy()*3.0)::Nil
//      )
  
  val ndarr2 = MXNDArray(ndarr.get.copy() * 2)
  println("ndarr2 =")
  ndarr2.get.toArray.foreach(println(_))
  
  val ndarrList = (
      (1,ndarr) :: (2, ndarr2) :: (3, MXNDArray(ndarr.get.copy()*3) ) :: Nil
      )
  
  println("ndarrList =")
  ndarrList.foreach(println)
  
  val ndarrDS = ndarrList.toDS() // !!!! ERROR !!!!
  
  ndarrDS.printSchema()
  
//  ndarrDS.map{
//    x =>  
//  }
  
  println("\n\n<<< DONE >>>")
}