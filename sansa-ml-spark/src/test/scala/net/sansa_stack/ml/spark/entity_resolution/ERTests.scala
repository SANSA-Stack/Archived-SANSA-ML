package net.sansa_stack.ml.spark.entity_resolution

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import net.sansa_stack.ml.spark.entity_resolution._

class ERTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.io._

  val source1 = getClass.getResource("/entity_resolution/source1.nt").getPath
  val source2 = getClass.getResource("/entity_resolution/source2.nt").getPath
  val lang = Lang.NTRIPLES
  val triplesSource1 = spark.rdf(lang)(source1)
  val triplesSource2 = spark.rdf(lang)(source2)
  val expected = Array("Ali_Baba : Ali_Baba", "Blackberry : Blackberry")
  val teacherRDD = spark.sparkContext.parallelize(expected)

  test("performing entity resolution using HashingTF method should result in 2 matches") {
    var outputRDD: RDD[(String, String, Double)] = null

    val erTest1 = new ERHashingTF(spark, triplesSource1, triplesSource2, 0.10, 0.50, 0.20, 6)
    outputRDD = erTest1.run()
    val predictedRDD = outputRDD.map(f => {
      (f._1 + " : " + f._2)
    })

    val cnt = teacherRDD.intersection(predictedRDD).count()
    assert(cnt==2)
  }

  test("performing entity resolution using CountVetcorizerModel method should result in 2 matches") {
    var outputRDD: RDD[(String, String, Double)] = null

    val erTest2 = new ERCountVectorizer(spark, triplesSource1, triplesSource2, 0.10, 0.50, 0.20, 8)
    outputRDD = erTest2.run()
    val predictedRDD = outputRDD.map(f => {
      (f._1 + " : " + f._2)
    })

    val cnt = teacherRDD.intersection(predictedRDD).count()
    assert(cnt==2)
  }

}


