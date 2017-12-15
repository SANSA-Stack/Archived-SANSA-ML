package net.sansa_stack.ml.spark.kge.linkprediction.NewDesign

import org.apache.spark.sql._

class TransE (spark : SparkSession,
              train : Dataset[RecordLongTriples],
              entities : Dataset[Long],
              relations : Dataset[Long],
              margin : Float,
              embeddingDimention : Int,
              batchSize : Int,
              epochSize : Int,
              learningRate : Float)    {
  
  
  
  def initialize() = {
    
//    train.mapPartitions(f, encoder)
    
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