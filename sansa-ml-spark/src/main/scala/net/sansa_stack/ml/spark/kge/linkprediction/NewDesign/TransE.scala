package net.sansa_stack.ml.spark.kge.linkprediction.NewDesign

import org.apache.spark.sql._

import scala.math._

import com.intel.analytics.bigdl.optim.Adam
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat


class TransE (spark : SparkSession,
              train : Dataset[RecordLongTriples],
              entities : Dataset[Long],
              relations : Dataset[Long],
              margin : Float,
              embeddingDimention : Int,
              batchSize : Int,
              epochSize : Int,
              learningRate : Float)    {
  
  
//  implicit val encoder = org.apache.spark.sql.Encoders.javaSerialization(classOf[Embedding])
  import spark.implicits._
  
  case class embedding( id : Long, ten : Tensor[Float] )
  
  implicit val encoder = org.apache.spark.sql.Encoders.javaSerialization(classOf[embedding])

  var lDS : Dataset[embedding] = null
  var eDS : Dataset[embedding] = null
  
  initialize()
  
  val x = lDS.map{
    emb : embedding => emb.id
  }.rdd.collect().foreach(println)
  
    
  def initialize() = {
    lDS = entities.map{
      id => embedding(id, Tensor(1,embeddingDimention).fill(0) )
    }
    
  }  
  
  def normalize() = {
    
  }
  
  def sample() = {
    
  }
  
  def corrupt() = {
    
  }
  
  def run() = {
    
  }
  
}