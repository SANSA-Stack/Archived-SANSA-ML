package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.mllib.linalg.Vector
import org.graphframes._
import org.graphframes.GraphFrame
import org.apache.spark.ml.evaluation.ClusteringEvaluator

/*
 *
 * Clustering
 * @triplesType - List of rdf subjects and their predicates.
 * @return - cluster of similar subjects.
 */

class LocalitySensitiveHashing(spark: SparkSession, nTriplesRDD: RDD[Triple], dir_path: String) extends Serializable {

  def run() = {
    val parsedTriples = getParsedTriples()
    val extractedEntity = getOnlyPredicates(parsedTriples)
    val featuredData_Df: DataFrame = vectoriseText(extractedEntity)
    val (model: MinHashLSHModel, transformedData_Df: DataFrame) = minHashLSH(featuredData_Df)
    val dataset: Dataset[_] = approxSimilarityJoin(model, transformedData_Df)
    clusterFormation(dataset, featuredData_Df)
  }

  def getParsedTriples(): RDD[(String, String, Object)] = {
    // Extracting last part of Triples
    return nTriplesRDD.distinct()
      .map(f => {   
        val s = f.getSubject.getURI.split("/").last
        val p = f.getPredicate.getURI.split("/").last

        if (f.getObject.isURI()) {
          val o = f.getObject.getURI.split("/").last
          (s, p, o)
        } else {
          val o = f.getObject.getLiteralValue
          (s, p, o)
        }
      })
  }

  def getOnlyPredicates(parsedTriples: RDD[(String, String, Object)]): RDD[(String, String)] = {
    return parsedTriples.map(f => {
      val key = f._1 + "" // Subject is the key
      val value = f._2 + ""  // Predicates are the values
      (key, value.replace(",", " ").stripSuffix(" ").distinct)
    }).reduceByKey(_ + " " + _)
  }

  def removeStopwords(tokenizedData_Df: DataFrame): DataFrame = {
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")
    val removed_df = remover.transform(tokenizedData_Df)
    return remover.transform(tokenizedData_Df)
  }

  def vectoriseText(entities: RDD[(String, String)]): DataFrame = {
    val entityProfile_Df = spark.createDataFrame(entities).toDF("entities", "attributes")
    val tokenizer = new Tokenizer().setInputCol("attributes").setOutputCol("words")
    val tokenizedData_Df = tokenizer.transform(entityProfile_Df)
    val cleanData_Df = removeStopwords(tokenizedData_Df).distinct
    val cleandfrdd = cleanData_Df.select("filtered_words").distinct.rdd
    val vocab_size = calculateVocabsize(cleandfrdd)
    val hashingTf = new HashingTF().setInputCol("filtered_words").
      setOutputCol("raw_Features").setNumFeatures(Math.round(0.90 * vocab_size).toInt)
    val isNonZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    val featuredData_hashedDf = hashingTf.transform(cleanData_Df).filter(isNonZeroVector(col("raw_Features")))
    val idf = new IDF().setInputCol("raw_Features").setOutputCol("features")
    val idfModel = idf.fit(featuredData_hashedDf)
    val rescaledHashedData = idfModel.transform(featuredData_hashedDf).
      filter(isNonZeroVector(col("features")))

    return rescaledHashedData
  }

  def calculateVocabsize(cleandfrdd: RDD[Row]): Int = {
    val vocab = cleandfrdd.map(_.mkString).reduce(_ + ", " + _).split(", ").toSet
    return (vocab.size)

  }

  def minHashLSH(featuredData_Df: DataFrame): (MinHashLSHModel, DataFrame) = {
    val mh = new MinHashLSH().setNumHashTables(3).setInputCol("features").setOutputCol("HashedValues")
    val model = mh.fit(featuredData_Df)
    val transformedData_Df = model.transform(featuredData_Df)
    return (model, transformedData_Df)
  }

  def approxSimilarityJoin(model: MinHashLSHModel, transformedData_Df: DataFrame): Dataset[_] = {
    val threshold = 0.40
    return model.approxSimilarityJoin(transformedData_Df, transformedData_Df, threshold)
  }

  def clusterFormation(dataset: Dataset[_], featuredData_Df: DataFrame) = {
    val featuredData = featuredData_Df.drop("attributes", "words", "filtered_words")

    val refined_entities_dataset = dataset
      .filter("datasetA.entities != datasetB.entities")
      .select(col("datasetA.entities").alias("src"), col("datasetB.entities").alias("dst"))

    import spark.implicits._
    val c_1 = refined_entities_dataset.select("src")
    val c_2 = refined_entities_dataset.select("dst")
    val vertexDF = c_1.union(c_2).distinct().toDF("id")

    val g = GraphFrame(vertexDF, refined_entities_dataset)
    g.persist()
    spark.sparkContext.setCheckpointDir(dir_path)
    
    // Connected Components are the generated clusters.
    val connected_components = g.connectedComponents.run()
    val connected_components_ = connected_components.withColumnRenamed("component", "prediction").
      withColumnRenamed("id", "entities")
    clusterQuality(connected_components_, featuredData)
  }
  
  /*
   * Calculating Silhouette score, which will tell how good clusters are.
   * Silhouette values ranges from [-1,1]. 
   * Values closer to 1 indicates better clusters
   */
  
  def clusterQuality(connectedComponents: DataFrame , featuredData: DataFrame) = {
    var silhouetteInput = connectedComponents.join(featuredData, "entities")
    val evaluator = new ClusteringEvaluator().setPredictionCol("prediction").
      setFeaturesCol("features").setMetricName("silhouette")
    val silhouette = evaluator.evaluate(silhouetteInput)   
  }

}

object LocalitySensitiveHashing {
  def apply(spark: SparkSession, nTriplesRDD: RDD[Triple], dir_path: String): LocalitySensitiveHashing = new LocalitySensitiveHashing(spark, nTriplesRDD, dir_path)
} 
