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


class MyClass(var ID : Int) extends Serializable {

	    var name = "test-" + ID
			var age = ID * 10
			var ten = Tensor(3,4).fill(ID)

			def getName() : String = {
					return name
			}

			def getAge() : Int = {
					return age
			}

			def getTen() : Tensor[Float]  = {
			  return ten
			}
			
			def getTenSize() : String = {
			  val n = ten.size().length 
			  var s : String = "("
			  var i = 1
			  for( i <- 0 to n-1) {
			    if (i == n-1) {
			      s += ten.size()(i).toString() + ")"
			    } else {
			      s += ten.size()(i).toString() + ","
			    }			    
			  }
			  return s			    
			}
			
			override def toString() : String = {
					"<ID=" + ID + ", age=" + age + ", name=" + name + ", tensor-size=" + getTenSize() + ">"     
			}
}


object playingSerialization2 {
	//extends App {
	def main(args: Array[String]): Unit = {
			Logger.getLogger("org").setLevel(Level.OFF)
			Logger.getLogger("akka").setLevel(Level.OFF)

			val spark = SparkSession.builder
			.master("local")
			.appName("TransE")
			.getOrCreate()

			import spark.implicits._

			
			val myclassSeq = Seq.range(1, 5).map(new MyClass(_))
			
			println("myclassSeq=", myclassSeq)
			//seq.foreach(println)
			
			implicit val encoder = org.apache.spark.sql.Encoders.javaSerialization(classOf[MyClass])
			
			val myclassDS = myclassSeq.toDS()
			
			myclassDS.printSchema()
			
			myclassDS.rdd
			         .collect()
			         .foreach(println)
			
			val ids = myclassDS.map{
			  case c : MyClass => c.ID
			}.asInstanceOf[Dataset[Int]]
			
			ids.collect()
			   .foreach(println)

			val myclassDS2 = myclassDS.map{
			  case c : MyClass => c.ten = Tensor(2,2).fill(c.ID)
			  c
			}.asInstanceOf[Dataset[MyClass]]
			
			myclassDS2.rdd
			          .collect()
			          .foreach(println)
			
			println("<< DONE >>")

	}
}