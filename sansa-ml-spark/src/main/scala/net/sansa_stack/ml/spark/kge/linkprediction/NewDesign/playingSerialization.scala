package net.sansa_stack.ml.spark.kge.linkprediction.NewDesign

import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.utils.T
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.beans


class SerTest(ID:Int) extends Serializable {

	    var name = "test-"+ID
			var age = ID*10
			var ten = Tensor(2,3).fill(ID)

			def getName() : String = {
					return name
			}

			def getAge() : Int = {
					return age
			}

			def getTen() : Tensor[Float]  = {
			  return ten
			}
			override def toString() : String = {
					"<ID="+ID+", age="+age+", name="+name+">"     
			}
}


object playingSerialization {
	//extends App {
	def main(args: Array[String]): Unit = {
			Logger.getLogger("org").setLevel(Level.OFF)
			Logger.getLogger("akka").setLevel(Level.OFF)

			val spark = SparkSession.builder
			.master("local")
			.appName("TransE")
			.getOrCreate()

			import spark.implicits._

			val lst = List(new SerTest(1), new SerTest(2), new SerTest(3))

			val rdd = spark.sparkContext.parallelize(lst,3)

			rdd.map(x => x.toString()).collect().foreach(println)
			println(rdd.getNumPartitions)

			// val enc = Encoders.bean[SerTest](classOf[SerTest])

			//  val ds = spark.createDataset(rdd , enc)



			implicit val kryoEncoder = Encoders.kryo(classOf[SerTest])

			val rdd2 = spark.sparkContext.parallelize(lst,2)
			val ds2 = rdd2.toDS()
			ds2.printSchema() // although in binary form but work as usual
			ds2.map( x => x.age ).collect().foreach(println)

			ds2.map{ x => 
			  val t = x
			  t.getName() }.collect().foreach(println)
			
			
			val t=Tensor(2,3).fill(1)+100
			println(t)
			
//			val x = new SerTest(1234)
//		
//			println(x.ten)
			
			
			//  val rdd2 = spark.sparkContext.parallelize(
			//      Array.fill(100){scala.util.Random.nextInt(100)} ,
			//      4)
			//  val ds2 = rdd2.toDS()
			//  ds2.show()
			
			rdd2.map{case s => s.ten = s.ten + 1000 }
		  rdd2.map{s => s.ten}.collect().foreach(println)  
			  
			//val ds3 = ds2.map{case s:SerTest => s.ten = s.ten + 1000 }.asInstanceOf[Dataset[SerTest]]
					
      //ds3.map{s => s.ten}.collect().foreach(println)
			
			println("<< DONE >>")

	}
}