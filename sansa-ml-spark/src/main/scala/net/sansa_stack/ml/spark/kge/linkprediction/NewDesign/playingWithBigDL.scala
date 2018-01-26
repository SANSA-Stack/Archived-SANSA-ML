package net.sansa_stack.ml.spark.kge.linkprediction.NewDesign

import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.utils.T
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object playingWithBigDL extends App {
  
	Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("TransE")
    .getOrCreate()
    
  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  println("<<< STARTING >>>")
  
  val ten = Tensor(3,4)+1
  
  println("sum elements = ",ten.sum() )
  println("ten cloned * 2 = ",ten.clone().mul(2)) 
	println("ten = ", ten)
	
	val tenList = (
      (1,ten)::(2,ten.clone().mul(2))::(3,ten.clone().mul(3))::Nil
      )
      
  println("tenList = ",tenList)
  
  case class encoded(id: Long, ten: Tensor[Float])
  
  val tenRDD = spark.sparkContext.parallelize(tenList,3)
  
  println("tenRDD = ", tenRDD)
  
  val sumRDD = tenRDD.map{
	  // x => x._2.sum()
	  case (id: Int, ten: Tensor[Float]) => ten.sum()	  
	}
  
	println("sumRDD = ")
	sumRDD.foreach(println)
	println()
	
	val tenRDD2 = tenRDD.mapValues{
	  ten  => (ten * 1000) / ten.sum()
	}

	println("tenRDD2 = ")
	tenRDD2.foreach(println)
	
  // creating a Dataset
	
//	val tenDS = tenRDD.toDS() // !!!! ERROR !!!!
	
//	tenDS.printSchema()
	

	
	
//	System.exit(0)

	
  println("<< DONE >>")
}