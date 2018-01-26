package net.sansa_stack.ml.spark.kge.linkprediction.NewDesign

class Embedding(ID : Long, 
                var dimEmbedding : Int
                
                ) extends Serializable {
  
  def this(ID : Long) {
    this(ID, 2)
  }
  
}