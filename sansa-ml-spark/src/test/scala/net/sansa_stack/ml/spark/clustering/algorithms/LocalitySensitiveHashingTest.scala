package net.sansa_stack.ml.spark.clustering.algorithms

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import net.sansa_stack.ml.spark.clustering.algorithms._

class LocalitySensitiveHashingTest extends FunSuite with DataFrameSuiteBase {
  
  import net.sansa_stack.rdf.spark.io._

  val path = getClass.getResource("/Cluster/testSample.nt").getPath
  val graphframeCheckpointPath = "/sansa-ml-spark_2.11/src/main/scala/net/sansa_stack/ml/spark/clustering/algorithms/graphframeCheckpoints"
  
  test("performing clustering using HashingTF method") {
    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val cluster_ = new LocalitySensitiveHashing(spark, triples, graphframeCheckpointPath)
    cluster_.run()
    assert(true)

  }
  
}