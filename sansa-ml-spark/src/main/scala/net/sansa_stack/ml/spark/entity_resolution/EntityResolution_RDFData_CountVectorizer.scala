package net.sansa_stack.ml.spark.entity_resolution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.spark.ml.feature.{ Tokenizer, HashingTF }
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.feature.MinHashLSHModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.Row
import org.apache.spark.RangePartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.RangePartitioner
import org.apache.spark.RangePartitioner

/* A generic Entity Resolution Approach for larger datasets(for e.g. 24.8GB)
 * Dataset1 has 47 million entities, approx 19GB
 * Dataset2 has 19 million entities, approx 5.8GB
 * */
class EntityResolution_RDFData_CountVectorizer(spark: SparkSession, triplesRDD1: RDD[Triple], triplesRDD2: RDD[Triple],
                                         teacher: DataFrame, threshold_subject: Double, jsimilarity_predicate: Double,
                                         threshold_object: Double, vocab_size: Long, output_path: String) extends Serializable {

  /**
   * Simple Api to call other apis
   * Execute entity profile generation
   * Step1- Find matching entities based on LSH subjects, by lsh_subjects api
   * Step2- Compare the predicates for matched entities, by get_similar_predicates api
   * Step3- Compare the objects for intersecting predicates in matched entities, by get_similar_objects api
   */
  def run(): RDD[(String, String)] = {

    // Define entity profiles from triplesRDD1 and triplesRDD2
    val entity_profiles1 = get_entity_profiles(triplesRDD1)
    val entity_profiles2 = get_entity_profiles(triplesRDD2)

    // Similar entities matched based on subjects
    val ds_subjects1: RDD[(String, String, String, String)] = lsh_subjects(entity_profiles1, entity_profiles2)
    val ds_subjects = ds_subjects1.repartition(600).persist(StorageLevel.MEMORY_AND_DISK)

    // Compare the predicates for matched entities by subject
    val refined_data_pred = get_similar_predicates(ds_subjects)
    val ds_predicates = refined_data_pred.repartition(400).persist(StorageLevel.MEMORY_AND_DISK)

    // Compare the objects for intersecting predicates in matched entities by predicate level knowledge
    val refined_objects = get_similarity_objects(ds_predicates)

    //Evaluate our results with groundtruth data
    val output = evaluation(refined_objects)
    
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
   * 
   * @param triplesRDD contains RDF data in the form of triples
   * @return ([(subject,predicate1:object1 , predicate2:object2 , ... , predicaten:objectn)]), 
   * where subject is the key and the group of paired predicate:object forms the value
   * ex:- (Budapest_City, areaCode:1 , country:Hungary)
   */
  def get_entity_profiles(triplesRDD: RDD[Triple]): RDD[(String, String)] = {

    //predicates to be filtered out from triples
    val removePredicates: List[String] = List("owl:sameas", "wikiPageID", "wikiPageRevisionID", "wikiPageRevisionLink",
      "wikiPageUsesTemplate", "wikiPageHistoryLink", "wikiPageExternalLink", "wikiPageEditLink", "wikiPageExtracted",
      "wikiPageLength", "wikiPageModified", "wikiPageOutDegree", "wikiPageRedirects")
      
    val broadcastVar = spark.sparkContext.broadcast(removePredicates) // broadcast here small RDD
    val distinct_triples = triplesRDD.distinct()
    val remove_triples = distinct_triples.mapPartitions({ f =>
      val k = broadcastVar.value
      for {
        x <- f
        z <- k
        if x.getPredicate.getURI().contains(z))
      } yield (x)
      
    })  
    
    val filtered_triples = distinct_triples.subtract(remove_triples)
    
    //Define entity profiles <subject, predicate1:object1 , predicate2:object2 , ... , predicaten:objectn>
    val entity = filtered_triples.filter(f => (f.getObject().isURI() || f.getObject.getLiteralLanguage == "en" || f.getObject.getLiteralLanguage == ""))
      .map(f => {
        val key = f.getSubject.getURI.split("/").last.trim()
        val pred = f.getPredicate.getURI.split(Array('/', '#')).last.trim()
        if (f.getObject.isURI()) {
          val obj = f.getObject.getURI.split("/").last.trim()
          val value = pred + ":" + obj //predicate and object are seperated by ':'
          (key, value)
        } else {
          val obj = f.getObject.getLiteral.toString().split(Array('^', '@')).head.trim()
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
   * 
   * Apply LSH technique on the tokenised subjects to get matched pairs on threshold_subject, specified by user
   * Join the predicate:object knowledge for each of the entity matches returned
   * 
   * @param entites_RDD1 and entitites_RDD2 contains the entity profiles, generated from get_entity_profiles, to be compared for match
   * @param threshold_subject - the similarity threshold set for approxsimilarityjoin
   * @return ([(entity1_subject, entity1_predicates:objectspairs, entity2_subject , entity2_predicates:objectspairs)]), 
   * where entity1_subject and entity2_subject are the matched pairs
   */
  def lsh_subjects(entites_RDD1: RDD[(String, String)], entities_RDD2: RDD[(String, String)]): RDD[(String, String, String, String)] = {

    //Get subject data and tokenise it
    val ent_sub1 = entites_RDD1.map(f => { (f._1, f._1.split("_")) })
    val part_rdd1 = new RangePartitioner(400, ent_sub1)
    val partitioned_rdd1 = ent_sub1.partitionBy(part_rdd1).persist(StorageLevel.MEMORY_AND_DISK)

    val ent_sub2 = entities_RDD2.map(f => { (f._1, f._1.split("_")) })
    val part_rdd2 = new RangePartitioner(400, ent_sub2)
    val partitioned_rdd2 = ent_sub2.partitionBy(part_rdd2).persist(StorageLevel.MEMORY_AND_DISK)

    val entities_Df1 = spark.createDataFrame(partitioned_rdd1).toDF("entities", "ent_sub")
    val entities_Df2 = spark.createDataFrame(partitioned_rdd2).toDF("entities", "ent_sub")

    //Apply LSH technique by HashingTF vectorisation
    val (cvfeatured_entities_Df1: DataFrame, cvfeatured_entities_Df2: DataFrame) = applyCountVectorizer_sub("ent_sub", "features", entities_Df1, entities_Df2)
    val (model_sub: MinHashLSHModel, transformed_sub_Df1: DataFrame, transformed_sub_Df2: DataFrame) = minHashLSH(cvfeatured_entities_Df1, cvfeatured_entities_Df2)
    val ds_subjects = approxsimilarityjoin(model_sub, transformed_sub_Df1, transformed_sub_Df2)
    
    //Combine predicate:object level knowledge for the matched pairs
    val ds_subjects_rdd = ds_subjects.rdd
    val ds_subjects_data1 = ds_subjects_rdd.map(f => { (f.get(0).toString(), f.get(1).toString()) }).join(entites_RDD1)
    val ds_subjects_data2 = ds_subjects_data1.map(f => { (f._2._1, (f._1, f._2._2)) }).join(entities_RDD2)
    val ds_subjects_data = ds_subjects_data2.map(f => { (f._2._1._1, f._2._1._2, f._1, f._2._2) })
    
    ds_subjects_data
  }

  /**
   * This api vectroises the entity subjects tokenised to form features
   * 
   * Apply CountVectorizer vectorisation on the tokenised subjects, setting our setVocabSize means that in our dictionary we will be adding approximately terms<=vocab_size. Terms are in the inp_column
   * 
   * @param inp_col specifies the input column for vectorisation
   * @param out_col specifies the output column containing features
   * data1 and data2 are dataframes containing the tokenised subjects
   * @return Dataframes with vectorised features i.e. tokenised subjects are vectorised here
   */
  def applyCountVectorizer_sub(inp_col: String, out_col: String, data1: DataFrame, data2: DataFrame): (DataFrame, DataFrame) = {
    val data = data1.union(data2).distinct()
    val countVectorizer =  new CountVectorizer().setInputCol(inp_col).setOutputCol(out_col).setVocabSize(vocab_size.toInt).setMinDF(1).fit(data)
    val cvfeatured_entities_Df1 = countVectorizer.transform(data1)
    val cvfeatured_entities_Df2 = countVectorizer.transform(data2)
    return (cvfeatured_entities_Df1, cvfeatured_entities_Df2)
  }

   /**
   * This api MinHashes the featured entity subjects
   * 
   * setting our setNumHashTables to 3 means 3 hashvalues to be generated for each feature
   * 
   * @param featured_entites_Df1 and featured_entites_Df2 specifies the featured dataframes generated by applyHashingTf_sub api
   * @return MinHashLSH model with Dataframes containing minhashes generated for the features
   */
  def minHashLSH(featured_entites_Df1: DataFrame, featured_entites_Df2: DataFrame): (MinHashLSHModel, DataFrame, DataFrame) = {
    val mh = new MinHashLSH().setNumHashTables(3).setInputCol("features").setOutputCol("hashed_values")
    val featured_data = featured_entites_Df1.union(featured_entites_Df2).distinct()
    val model = mh.fit(featured_data)
    val transformed_entities_Df1 = model.transform(featured_entites_Df1)
    val transformed_entities_Df2 = model.transform(featured_entites_Df2)/*
 * 
 * */
    return (model, transformed_entities_Df1, transformed_entities_Df2)
  }

  /**
   * This api applies approxsimilarity join to detect entity matches with subject similarity
   * 
   * Applying approxSimilarityJoin with threshold specified by user on subjects
   * A lower threshold means the entity matches found are closely related
   * 
   * @param model - MinHashLSHModel generated by minHashLSH api
   * @param df1 and df2 specifies the dataframes, generated by minHashLSH api
   * @param threshold- threshold for subject similarity specified by user
   * @return matched entity pairs
   */
  def approxsimilarityjoin(model: MinHashLSHModel, df1: DataFrame, df2: DataFrame): DataFrame = {
    val dataset = model.approxSimilarityJoin(df1, df2, threshold_subject)
    val refined_ds = dataset.select(col("datasetA.entities").alias("entity1"), col("datasetB.entities").alias("entity2")) //only for lsh1subjects
    refined_ds
  }

   /**
   * This api compares predicate level knowledge of similar entities matched pairs generated by lsh_subjects api
   * Compute jaccard similarity on the predicates of paired entity matches
   * Filter the entity matches with similarity more than jSimilarity, specified by user
   * 
   * @param similar_subj_rdd contains the entity matches based on subjects with intergated attribute level knowledge, generated from lsh_subjects
   * @param jSimilartiy - the Jaccard similarity threshold set for predicate level comparison
   * @return ([(entity1_subject, entity1_predicates:objectspairs, entity2_subject, entity2_predicates:objectspairs, intersecting_predicates, jsimilarityofpredicates)]), 
   * where entity1_subject and entity2_subject are the matched pairs on predicate level knowledge
   */
  def get_similar_predicates(similar_subj_rdd: RDD[(String, String, String, String)]): RDD[(String, List[String], String, List[String], List[String], Double)] = {
    val refined_data_sub = similar_subj_rdd.map(f => {
      val sub1 = f._1 // entity1_subject
      val s_data1 = f._2 // entity1_predicateobject_pairs
      val sub2 = f._3 // entity2_subject
      val s_data2 = f._4 //// entity2_predicateobject_pairs
      
      //segregate each of the predicate_object pairs for both the entities
      val pred_obj1 = s_data1.split(" , ").toList 
      val pred_obj2 = s_data2.split(" , ").toList
      
      //empty lists for predicates
      var list_pred1 = List[String]()
      var list_pred2 = List[String]()
      
      //extract only predicates from the predicate_object for both entities for comparison
      for (x <- pred_obj1) {
        list_pred1 = list_pred1 :+ x.split(":").head
      }
      for (x <- pred_obj2) {
        list_pred2 = list_pred2 :+ x.split(":").head
      }
      
      //Find common predicates among the entities
      val intersect_pred = list_pred1.intersect(list_pred2)
      val union_pred = list_pred1.length + list_pred2.length - intersect_pred.length
      
      //calculate jaccard similarity on predicate level knowledge of both entities for comparison
      val similarity = intersect_pred.length.toDouble / union_pred.toDouble

      (sub1, pred_obj1, sub2, pred_obj2, intersect_pred, similarity)
    })
    similar_subj_rdd.unpersist()
    
    //filter the entity pairs with jaccard similarities that fit or are above user defined jsimilarity for predicate level knoledge comparison
    val refined_data_pred = refined_data_sub.filter(f => f._6 >= jSimilartiy_predicate)
    
    refined_data_pred
  }

  /**
   * This api removes false positives by compares object level knowledge of similar entities matched pairs generated by get_similar_predicates api
   * Compute jaccard similarity on the objects of paired entity matches, only for the intersecting predicates
   * Filter the entity matches with similarity more than threshold_objects, specified by user
   * 
   * @param ds_pred contains the entity matches based on predicate level knowledge
   * @param threshold_objects - the Jaccard similarity threshold set for object level comparison
   * @return ([(entity1_subject, entity2_subject, jsimilarityofobjects)]), 
   * where entity1_subject and entity2_subject are the matched pairs on object level knowledge
   */
  def get_similarity_objects(ds_pred: RDD[(String, List[String], String, List[String], List[String], Double)]): RDD[(String, String, Double)] = {
    val mapped_objects = ds_pred.map(f => {
      val sub1 = f._1 // entity1_subject
      val pred_obj1 = f._2 // entity1_predicateobject_pairs
      val sub2 = f._3 // entity2_subject
      val pred_obj2 = f._4 // entity2_predicateobject_pairs
      val common_pred = f._5 // intersecting_predicates of both entites for comparing their objects

      var obj1: String = " "
      var obj2: String = " "

      // Segregate objects of only intersecting predicates among the two entities
      for (x <- pred_obj1) {
        val pred = x.split(":").head
        val obj = x.split(":").last
        if (common_pred.contains(pred))
          obj1 = obj1 + " " + obj
      }

      for (x <- pred_obj2) {
        val pred = x.split(":").head
        val obj = x.split(":").last
        if (common_pred.contains(pred))
          obj2 = obj2 + " " + obj
      }

      val sub_obj1 = obj1.trim().split(" ").toList.distinct
      val sub_obj2 = obj2.trim().split(" ").toList.distinct

      //Compute jaccard similarity on the objects
      val intersect_obj = sub_obj1.intersect(sub_obj2).length
      val union_obj = sub_obj1.length + sub_obj2.length - intersect_obj

      val similarity = intersect_obj.toDouble / union_obj.toDouble

      (sub1, sub2, similarity)
    })
    ds_pred.unpersist()
    // Extract entity matches with similarity more than threshold_objects, specified by user
    val results = mapped_objects.filter(f => f._3 >= threshold_object)
    
    results
  }

  /**
   * This api evaluates our results by comparing it with groundtruth
   * Compute Precision, Recall and F1-Measure
   * 
   * @param result contains the entity matches predicted by our algorithm, generated from get_similarity_objects api
   * @param teacher - the groundtruth for comparison
   * @param output_path - path to save the result rdd
   */
  def evaluation(result: RDD[(String, String, Double)]): RDD[(String, String)] = {
    val predicted_rdd = result.map(f => {
      (f._1, f._2)
    })
    val teacher_rdd = teacher.rdd
    val actual_rdd = teacher_rdd.map(f => {
      (f.get(0).toString(), f.get(1).toString())
    })
    
    //Calculate TruePostives for precision, recall and f1-measure
    val truePositives = actual_rdd.intersection(predicted_rdd).count
    println("***************************************************************************************")
    println("***************************************************************************************")

    val actual = actual_rdd.count()
    val results = predicted_rdd.count()
    println("Actual: " + actual)
    println("Predicted: " + results)
    println("True Positives: " + truePositives)
    val precision = (truePositives * 100.00) / (results)
    println("Precision: " + precision)
    val recall = (truePositives * 100) / (actual)
    println("Recall: " + recall)
    val f1_measure = (2 * precision * recall) / (precision + recall)
    println("F1-measure: " + f1_measure)
    println("***************************************************************************************")
    println("***************************************************************************************")
    //Save the output_rdd
    predicted_rdd.coalesce(1).saveAsTextFile(output_path)
    println("Output Saved!")
    
    predicted_rdd
  }
}

object EntityResolution_RDFData_CountVectorizer {
  def apply(spark: SparkSession, triplesRDD1: RDD[Triple], triplesRDD2: RDD[Triple],
            teacher: DataFrame, threshold_subject: Double, jsimilarity_predicate: Double,
            threshold_object: Double, vocab_size: Long, output_path: String): EntityResolution_RDFData_CountVectorizer = new EntityResolution_RDFData_CountVectorizer(spark, triplesRDD1, triplesRDD2,
    teacher, threshold_subject, jsimilarity_predicate, threshold_object, vocab_size, output_path)
}
