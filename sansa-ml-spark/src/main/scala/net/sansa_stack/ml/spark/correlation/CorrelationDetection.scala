package net.sansa_stack.ml.spark.correlation

import org.apache.jena.query.{QueryExecutionFactory, QueryFactory}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.util.FileManager
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.collection.mutable

/*
 *
 * Correlation - Correlation Detection on RDF data
 * @correlationConf - a configuration file guiding the correlation
 * @correlationType - type of correlation ["spearman", "pearson"]
 * @return - correlation Double value.
 */
class CorrelationDetection(correlationConf: String, correlationType: String, spark: SparkSession) {

  def run(): Double = {   

    // Data source 1
    val properties = getProperties(correlationConf)
    val source1 = properties(0).source
    val target1 = properties(0).target
    val filters1 = properties(0).filters.toList.to[mutable.ListBuffer]

    // Data source 2
    val source2 = properties(1).source
    val target2 = properties(1).target
    val filters2 = properties(1).filters.toList.to[mutable.ListBuffer]

    // Extract values from source 1
    val results1 = getValues(filters1, target1, source1)
    println(results1.mkString("_"))

    // Extract values from source 2
    val results2 = getValues(filters2, target2, source2)
    println(results2.mkString("_"))

    // seriesX and seriesY must have the same number of partitions and cardinality
    val seriesX: RDD[Double] = spark.sparkContext.parallelize(results1)
    val seriesY: RDD[Double] = spark.sparkContext.parallelize(results2)

    // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
    val correlation: Double = Statistics.corr(seriesX, seriesY, correlationType)
    println(s"Correlation is: $correlation")

    correlation
  }

  // Methods
  def getValues(filters: mutable.ListBuffer[(String, String)], target: String, source: String): mutable.ListBuffer[Double] = {
    var singleFilter, multiFilters = mutable.ListBuffer[(String, String)]()
    var aggregateFnc = ""

    for (f <- filters) {
      val prop = f._1
      if (!f._2.contains("{")) {
        singleFilter += ((prop, f._2))
      } else {
        val bits = f._2.split(";")
        val vals = bits(0).split("""}\{""")
        aggregateFnc = bits(1)
        for (v <- vals) {
          val vWithoutBraces = v.replace("{","").replace("}","")
          multiFilters += ((prop, vWithoutBraces))
        }
      }
    }

    calculateResults(singleFilter, multiFilters, filters, target, source, aggregateFnc)
  }

  def calculateResults(singles: mutable.ListBuffer[(String, String)],
                       multiples: mutable.ListBuffer[(String, String)],
                       filters: mutable.ListBuffer[(String, String)],
                       target: String, source: String, aggregateFnc: String): mutable.ListBuffer[Double] = {

    var values = mutable.ListBuffer[Double]()
    if (multiples.isEmpty) {
      println("Single set of values.")
      val (select, where) = getFilters(filters)
      val queryString = makeQuery(target, select, where)
      println("query: " + queryString)
      values = runQuery(queryString, source)
      println("values: " + values.mkString("_"))
    } else {
      println("Multiple sets of values.")
      val singleFilter: mutable.ListBuffer[(String, String)] = singles

      for (filters <- multiples) {
        println("Obtaining results for: " + filters._1 + ", " + filters._2)
        singleFilter += ((filters._1,filters._2)) // E.g., ListBuffer((dbpedia:year,2015), (dbpedia:month,3,4,5))
        val (select, where) = getFilters(singleFilter)
        // E.g., select = (?s dbpedia:year ?f1 . ?s dbpedia:month ?f2)
        // E.g., where = FILTER (?f1 = 2015) . FILTER (?f2 IN (3,4,5))
        val queryString = makeQuery(target, select, where)
        println("query: " + queryString)
        val results = runQuery(queryString, source)
        println("Results: " + results)

        var aggregate: Double = 0.0
        if (aggregateFnc == "sum") aggregate = sum(results)

        values += aggregate

        singleFilter -= ((filters._1,filters._2)) // So not to append new (a,b,c)
      }
    }

    values
  }

  def sum(results: mutable.ListBuffer[Double]): Double = {
    var s: Double = 0
    for (v <- results)
      s += v

    s/results.length
  }

  def getFilters(filters: mutable.ListBuffer[(String, String)]) = {
    var select = ""; var where = ""; var i = 1
    for (f <- filters) {
      val prop = f._1
      val obj = f._2
      select += s"?s $prop ?f$i . "
      var ss = ""
      for (i <- obj.split(",")){
        ss += s""""$i","""
      }
      println("ddddd: " + ss)
      if (obj.contains(",")) // Multiple values
        where += s"FILTER (?f$i IN (${ss.dropRight(1)})) ."
      else
        where += s"""FILTER (?f$i = "$obj") . """
      i += 1
    }
    (select, where)
  }

  def makeQuery(target: String, select: String, where: String): String = {
    val queryString = "PREFIX qb: <http://purl.org/linked-data/cube#>" +
      //"PREFIX eg: <http://example.org/ns#>" +
      "SELECT ?value WHERE {" +
        s"?s $target ?value . " +
        select +
        where +
      "} ORDER BY ?f2" // TODO: to automate this, pass ?f2 as method output

    queryString
  }

  def runQuery(queryString: String, source: String) = {
    val query = QueryFactory.create(queryString)

    val in = FileManager.get().open(source)
    if (in == null) {
      throw new IllegalArgumentException("File: " + source + " not found")
    }

    val model = ModelFactory.createDefaultModel()
    model.read(in, null, "TURTLE")

    val qe = QueryExecutionFactory.create(query, model)
    val results = qe.execSelect()

    var values = mutable.ListBuffer[Double]()

    while(results.hasNext) {
      val soln = results.nextSolution()
      val res = soln.get("value").asLiteral().getDouble
      values = values :+ res
    }

    values
  }

  def getProperties(path: String) = {

    val properties = scala.io.Source.fromFile(path)
    val configJSON = try properties.mkString finally properties.close()

    case class ConfigObject(source: String, target: String, filters: Map[String,String])

    implicit val userReads: Reads[ConfigObject] = (
      (__ \ 'source).read[String] and
      (__ \ 'target).read[String] and
      (__ \ 'filters).read[Map[String,String]]
    )(ConfigObject)

    val sources = (Json.parse(configJSON) \ "properties").as[Seq[ConfigObject]]

    sources
  }

  def testQuery(queryString: String, source: String) = {
    val query = QueryFactory.create(queryString)

    val in = FileManager.get().open(source)
    if (in == null) {
      throw new IllegalArgumentException("File: " + source + " not found")
    }

    val model = ModelFactory.createDefaultModel()
    model.read(in, null, "TURTLE")

    val qe = QueryExecutionFactory.create(query, model)
    val results = qe.execSelect()

    var values = mutable.ListBuffer[Double]()

    while(results.hasNext) {
      val soln = results.nextSolution()
      val res = soln.get("value").asLiteral().getDouble
      println("res: " + res)
      values = values :+ res
    }
  }
}

object CorrelationDetection {

  def apply(correlationConf: String, correlationType: String, sparkSession: SparkSession): 
    CorrelationDetection = new CorrelationDetection(correlationConf, correlationType, sparkSession)
  }

}