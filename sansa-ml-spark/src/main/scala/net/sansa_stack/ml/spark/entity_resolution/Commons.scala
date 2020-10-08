package net.sansa_stack.ml.spark.entity_resolution

import java.io.Serializable

import org.apache.jena.graph.Triple
import org.apache.spark.RangePartitioner
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.feature.MinHashLSHModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import scala.io.Source

/**
 *  A generic Entity Resolution Approach for larger datasets(for e.g. 24.8GB)
 * Dataset1 has 47 million entities, approx 19GB
 * Dataset2 has 19 million entities, approx 5.8GB
 */
abstract class Commons(val spark: SparkSession, val sourceData1: RDD[Triple], val sourceData2: RDD[Triple],
                                         val thresholdSubject: Double, val jsimilarityPredicate: Double,
                                         val thresholdObject: Double, val vocabSize: Long) extends Serializable {

  /**
   * Execute entity profile generation
   * Step1- Find matching entities based on LSH subjects, by lsh_subjects api
   * Step2- Compare the predicates for matched entities, by get_similar_predicates api
   * Step3- Compare the objects for intersecting predicates in matched entities, by get_similar_objects api
   */
  def run(): RDD[(String, String, Double)] = {
    import com.typesafe.config._
    val conf = ConfigFactory.load("application.conf")
    val rpredicates = conf.getStringList("sansa.entity_resolution.removePredicatesList")
    import scala.collection.JavaConverters._
    val removePredicates = rpredicates.asScala.toList
    val partitions = conf.getInt("sansa.entity_resolution.partitions")
    val repartitionNumber = conf.getInt("sansa.entity_resolution.repartition_number")
    // Define entity profiles from triplesRDD1 and triplesRDD2
    val entityProfiles1 = getEntityProfiles(sourceData1, removePredicates)
    val entityProfiles2 = getEntityProfiles(sourceData2, removePredicates)
    // Similar entities matched based on subjects
    val dsSubjects1: RDD[(String, String, String, String)] = lshSubjects(entityProfiles1, entityProfiles2, partitions)
    val dsSubjects = dsSubjects1.repartition(repartitionNumber).persist(StorageLevel.MEMORY_AND_DISK)
    // Compare the predicates for matched entities by subject
    val refinedDataPred = getSimilarPredicates(dsSubjects)
    val dsPredicates = refinedDataPred.repartition(partitions).persist(StorageLevel.MEMORY_AND_DISK)
    // Compare the objects for intersecting predicates in matched entities by predicate level knowledge
    val output = getSimilarityObjects(dsPredicates)
    output
    }
  /**
   * Filters triples and defines entity profiles.
   * The triplesRDD needs filteration. We perform data cleansing by removing common wikilinks predicates listed in removePredicates List
   * and owl:sameas triples
   * This api further uses map partition broadcasting the List remospark, triplesRDD1spark, triplesRDD1vePredicates, to find triples containing the listed predicates in distinct_triples rdd
   * Finally, subtract those triples to get filtered_triples RDD
   * Consider only triples with objects as URI or with "en" literal language or no literal language
   * Group all triples of a particular subject to form the entity profiles in the format:
   * <subject,predicate1:object1 , predicate2:object2 , ... , predicaten:objectn>
   * @param triplesRDD contains RDF data in the form of triples
   * @return ([(subject,predicate1:object1 , predicate2:object2 , ... , predicaten:objectn)]),
   * where subject is the key and the group of paired predicate:object forms the value
   * ex:- (Budapest_City, areaCode:1 , country:Hungary)
   */
  def getEntityProfiles(sourceData: RDD[Triple], removePredicates: List[String]): RDD[(String, String)] = {
    // predicates to be filtered out from triples
    val broadcastVar = spark.sparkContext.broadcast(removePredicates) // broadcast here small RDD
    val distinctTriples = sourceData.distinct()
    val removeTriples = distinctTriples.mapPartitions({ f =>
      val k = broadcastVar.value
      for {
        x <- f
        z <- k
        if x.getPredicate.getURI().contains(z)
      } yield (x)
      })
      val filteredTriples = distinctTriples.subtract(removeTriples)
      // Define entity profiles <subject, predicate1:object1 , predicate2:object2 , ... , predicaten:objectn>
      val entity = filteredTriples.filter(f => (f.getObject().isURI() || f.getObject.getLiteralLanguage == "en" || f.getObject.getLiteralLanguage == ""))
      .map(f => {
        val key = f.getSubject.getLocalName
        val pred = f.getPredicate.getLocalName
        if (f.getObject.isURI()) {
          val obj = f.getObject.getLocalName
          val value = pred + ":" + obj // predicate and object are seperated by ':'
          (key, value)
        } else {
          val obj = f.getObject.getLiteral.getLexicalForm()
          val value = pred + ":" + obj.replace(":", "")
          (key, value)
        }
      }).reduceByKey(_ + " , " + _) // triples seperated by ' , '
      entity
  }
  /**
   * This api matches similar entities based on similarity of their subjects
   * Get subject data from entites_RDD1 and entites_RDD2.
   * Tokenise it by "_" to form ent_sub1 and ent_sub2 for comparison
   * ex:- (Budapest_City, Set(Budapest, City))
   * Apply LSH technique on the tokenised subjects to get matched pairs on threshold_subject, specified by user
   * Join the predicate:object knowledge for each of the entity matches returned
   * @param entites_RDD1 and entitites_RDD2 contains the entity profiles, generated from get_entity_profiles, to be compared for match
   * @param threshold_subject - the similarity threshold set for approxsimilarityjoin
   * @return ([(entity1_subject, entity1_predicates:objectspairs, entity2_subject , entity2_predicates:objectspairs)]),
   * where entity1_subject and entity2_subject are the matched pairs
   */
  def lshSubjects(entitesRDD1: RDD[(String, String)], entitiesRDD2: RDD[(String, String)], partitions: Int): RDD[(String, String, String, String)] = {
    // Get subject data and tokenise it
    val entSub1 = entitesRDD1.map(f => { (f._1, f._1.split("_")) })
    val partrdd1 = new RangePartitioner(partitions, entSub1)
    val partitionedrdd1 = entSub1.partitionBy(partrdd1).persist(StorageLevel.MEMORY_AND_DISK)

    val entSub2 = entitiesRDD2.map(f => { (f._1, f._1.split("_")) })
    val partrdd2 = new RangePartitioner(partitions, entSub2)
    val partitionedrdd2 = entSub2.partitionBy(partrdd2).persist(StorageLevel.MEMORY_AND_DISK)

    val entitiesDf1 = spark.createDataFrame(partitionedrdd1).toDF("entities", "ent_sub")
    val entitiesDf2 = spark.createDataFrame(partitionedrdd2).toDF("entities", "ent_sub")

    // Apply LSH technique by vectorisation through HashingTF or CountVectorizer
    val (featuredEntitiesDf1: DataFrame, featuredEntitiesDf2: DataFrame) = vectorise("ent_sub", "features", entitiesDf1, entitiesDf2)
    val (modelSub: MinHashLSHModel, transformedSubDf1: DataFrame, transformedSubDf2: DataFrame) = minHashLSH(featuredEntitiesDf1, featuredEntitiesDf2)
    val dsSubjects = approxSimilarityJoin(modelSub, transformedSubDf1, transformedSubDf2)
    // Combine predicate:object level knowledge for the matched pairs
    val dsSubjectsRDD = dsSubjects.rdd
    val dsSubjectsData1 = dsSubjectsRDD.map(f => { (f.get(0).toString(), f.get(1).toString()) }).join(entitesRDD1)
    val dsSubjectsData2 = dsSubjectsData1.map(f => { (f._2._1, (f._1, f._2._2)) }).join(entitiesRDD2)
    val dsSubjectsData = dsSubjectsData2.map(f => { (f._2._1._1, f._2._1._2, f._1, f._2._2) })
    dsSubjectsData
    }

  def vectorise(inpCol: String, outCol: String, data1: DataFrame, data2: DataFrame): (DataFrame, DataFrame) // abstract method
  /**
   * This api MinHashes the featured entity subjects
   * setting our setNumHashTables to 3 means 3 hashvalues to be generated for each feature
   * @param featured_entites_Df1 and featured_entites_Df2 specifies the featured dataframes generated by applyHashingTf_sub api
   * @return MinHashLSH model with Dataframes containing minhashes generated for the features
   */
  def minHashLSH(featuredEntitesDf1: DataFrame, featuredEntitesDf2: DataFrame): (MinHashLSHModel, DataFrame, DataFrame) = {
    val mh = new MinHashLSH().setNumHashTables(3).setInputCol("features").setOutputCol("hashed_values")
    val featuredData = featuredEntitesDf1.union(featuredEntitesDf2).distinct()
    val model = mh.fit(featuredData)
    val transformedEntitiesDf1 = model.transform(featuredEntitesDf1)
    val transformedEntitiesDf2 = model.transform(featuredEntitesDf2)
    return (model, transformedEntitiesDf1, transformedEntitiesDf2)
  }

  /**
   * This api applies approxsimilarity join to detect entity matches with subject similarity
   * Applying approxSimilarityJoin with threshold specified by user on subjects
   * A lower threshold means the entity matches found are closely related
   * @param model - MinHashLSHModel generated by minHashLSH api
   * @param df1 and df2 specifies the dataframes, generated by minHashLSH api
   * @param threshold- threshold for subject similarity specified by user
   * @return matched entity pairs
   */
  def approxSimilarityJoin(model: MinHashLSHModel, df1: DataFrame, df2: DataFrame): DataFrame = {
    val dataset = model.approxSimilarityJoin(df1, df2, thresholdSubject)
    val refinedDs = dataset.select(col("datasetA.entities").alias("entity1"), col("datasetB.entities").alias("entity2")) // only for lsh1subjects
    refinedDs
  }

   /**
   * This api compares predicate level knowledge of similar entities matched pairs generated by lsh_subjects api
   * Compute jaccard similarity on the predicates of paired entity matches
   * Filter the entity matches with similarity more than jSimilarity, specified by user
   * @param similar_subj_rdd contains the entity matches based on subjects with intergated attribute level knowledge, generated from lsh_subjects
   * @param jSimilartiy - the Jaccard similarity threshold set for predicate level comparison
   * @return ([(entity1_subject, entity1_predicates:objectspairs, entity2_subject, entity2_predicates:objectspairs, intersecting_predicates, jsimilarityofpredicates)]),
   * where entity1_subject and entity2_subject are the matched pairs on predicate level knowledge
   */
  def getSimilarPredicates(similarSubjRDD: RDD[(String, String, String, String)]): RDD[(String, List[String], String, List[String], List[String], Double)] = {
    val refinedDataSub = similarSubjRDD.map(f => {
      val sub1 = f._1 // entity1_subject
      val sdata1 = f._2 // entity1_predicateobject_pairs
      val sub2 = f._3 // entity2_subject
      val sdata2 = f._4 // entity2_predicateobject_pairs
      // segregate each of the predicate_object pairs for both the entities
      val predObj1 = sdata1.split(" , ").toList
      val predObj2 = sdata2.split(" , ").toList
      // empty lists for predicates
      var listPred1 = List[String]()
      var listPred2 = List[String]()
      // extract only predicates from the predicate_object for both entities for comparison
      for (x <- predObj1) {
        listPred1 = listPred1 :+ x.split(":").head
      }
      for (x <- predObj2) {
        listPred2 = listPred2 :+ x.split(":").head
      }
      // Find common predicates among the entities
      val intersectPred = listPred1.intersect(listPred2)
      val unionPred = listPred1.length + listPred2.length - intersectPred.length
      // calculate jaccard similarity on predicate level knowledge of both entities for comparison
      val similarity = intersectPred.length.toDouble / unionPred.toDouble

      (sub1, predObj1, sub2, predObj2, intersectPred, similarity)
    })
    similarSubjRDD.unpersist()
    // filter the entity pairs with jaccard similarities that fit or are above user defined jsimilarity for predicate level knoledge comparison
    val refinedDataPred = refinedDataSub.filter(f => f._6 >= jsimilarityPredicate)
    refinedDataPred
  }

  /**
   * This api removes false positives by compares object level knowledge of similar entities matched pairs generated by get_similar_predicates api
   * Compute jaccard similarity on the objects of paired entity matches, only for the intersecting predicates
   * Filter the entity matches with similarity more than threshold_objects, specified by user
   * @param ds_pred contains the entity matches based on predicate level knowledge
   * @param threshold_objects - the Jaccard similarity threshold set for object level comparison
   * @return ([(entity1_subject, entity2_subject, jsimilarityofobjects)]),
   * where entity1_subject and entity2_subject are the matched pairs on object level knowledge
   */
  def getSimilarityObjects(dsPred: RDD[(String, List[String], String, List[String], List[String], Double)]): RDD[(String, String, Double)] = {
    val mappedObjects = dsPred.map(f => {
      val sub1 = f._1 // entity1_subject
      val predObj1 = f._2 // entity1_predicateobject_pairs
      val sub2 = f._3 // entity2_subject
      val predObj2 = f._4 // entity2_predicateobject_pairs
      val commonPred = f._5 // intersecting_predicates of both entites for comparing their objects

      var obj1: String = " "
      var obj2: String = " "

      // Segregate objects of only intersecting predicates among the two entities
      for (x <- predObj1) {
        val pred = x.split(":").head
        val obj = x.split(":").last
        if (commonPred.contains(pred)) {
          obj1 = obj1 + " " + obj
        }
      }

      for (x <- predObj2) {
        val pred = x.split(":").head
        val obj = x.split(":").last
        if (commonPred.contains(pred)) {
          obj2 = obj2 + " " + obj
        }
      }

      val subObj1 = obj1.trim().split(" ").toList.distinct
      val subObj2 = obj2.trim().split(" ").toList.distinct

      // Compute jaccard similarity on the objects
      val intersectObj = subObj1.intersect(subObj2).length
      val unionObj = subObj1.length + subObj2.length - intersectObj

      val similarity = intersectObj.toDouble / unionObj.toDouble

      (sub1, sub2, similarity)
    })
    dsPred.unpersist()
    // Extract entity matches with similarity more than threshold_objects, specified by user
    val results = mappedObjects.filter(f => f._3 >= thresholdObject)
    results
  }
}
