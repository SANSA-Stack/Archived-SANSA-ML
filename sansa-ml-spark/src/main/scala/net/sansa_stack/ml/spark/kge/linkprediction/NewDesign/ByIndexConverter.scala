package net.sansa_stack.ml.spark.kge.linkprediction.NewDesign

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._ 


class ByIndexConverter( triples : Triples,
                        spark : SparkSession) extends ConverterTrait {

  import spark.implicits._
  
  // The following does not generate consecutive indices !
  // val entities = triples.getAllDistinctEntities().withColumn("ID", monotonicallyIncreasingId ).persist()
  
  //protected
  val entities = {
    var temp = spark.createDataset[(String,Long)]( triples.getAllDistinctEntities().rdd.zipWithIndex() )
    val names = temp.columns
    temp.withColumnRenamed(names(0),"Entities").withColumnRenamed(names(1),"ID").persist()
  }
  
  
  // The following does not generate consecutive indices !
  // val predicates = triples.getAllDistinctPredicates().withColumn("ID", monotonicallyIncreasingId ).persist()

  //protected
  val predicates = {
    var temp = spark.createDataset[(String,Long)]( triples.getAllDistinctPredicates().rdd.zipWithIndex() )
    var names = temp.columns
    temp.withColumnRenamed(names(0),"Predicates").withColumnRenamed(names(1),"ID").persist()
  }
  
  
  def getTriplesByIndexOld(dsTriplesInString : Dataset[RecordStringTriples]) : Dataset[RecordLongTriples] = {
   
    val colNames = dsTriplesInString.columns
    val sub = colNames(0)
    val pred = colNames(1)
    val obj = colNames(2)
    
    val result = dsTriplesInString.withColumnRenamed(sub, "Subject")
                   .withColumnRenamed(pred,"Predicate")
                   .withColumnRenamed(obj, "Object")
                   .join(entities, col("Subject") === entities("Entities"), "left_outer")
                   .drop("Subject","Entities")
                   .withColumnRenamed("ID", "SubjectsID")
                   .join(predicates, col("Predicate") === predicates("Predicates") , "left_outer")
                   .drop("Predicate","Predicates")
                   .withColumnRenamed("ID","PredicatesID")
                   .join(entities, col("Object") === entities("Entities"), "left_outer")
                   .drop("Object","Entities")
                   .withColumnRenamed("ID","ObjectsID")
                   
    return result.asInstanceOf[Dataset[RecordLongTriples]]
  }
  
  def getTriplesByIndex(dsTriplesInString : Dataset[RecordStringTriples]) : Dataset[RecordLongTriples] = {
   
   val entitiesMap = spark.sparkContext.broadcast(
           entities.collect().map{
              row => (row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[Long])
           }.toMap
           )
           
   val predicatesMap = spark.sparkContext.broadcast(
           predicates.collect().map{
              row => (row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[Long])
           }.toMap
           )
           
//   val result0 = dsTriplesInString.mapPartitions{
//                 iter => iter.map{
//                             case trp : RecordStringTriples => RecordLongTriples(entitiesMap.value.get(trp.Subject).get,
//                                                                                 predicatesMap.value.get(trp.Predicate).get,
//                                                                                 entitiesMap.value.get(trp.Object).get )
//                 }
//   }
           
           
   val result = dsTriplesInString.map{
                  case trp : RecordStringTriples => RecordLongTriples(entitiesMap.value.get(trp.Subject).get,
                                                                                 predicatesMap.value.get(trp.Predicate).get,
                                                                                 entitiesMap.value.get(trp.Object).get )
   }        
   
   return result
 }

  def getTriplesByStringOld(dsTriplesInLong : Dataset[RecordLongTriples]) : Dataset[RecordStringTriples] = {
       
    val colNames = dsTriplesInLong.columns
    val sub = colNames(0)
    val pred = colNames(1)
    val obj = colNames(2)
    
//    val result = dsTriplesInLong.withColumnRenamed(sub, "SubjectID")
//                   .withColumnRenamed(pred,"PredicateID")
//                   .withColumnRenamed(obj, "ObjectID")
//                   .join(entities, col("SubjectID") === entities("ID"), "left_outer")
//                   .drop("SubjectID","ID")
//                   .withColumnRenamed("Entities", "Subjects")
//                   .join(predicates, col("PredicateID") === predicates("ID") , "left_outer")
//                   .drop("PredicateID","ID")
//                   .withColumnRenamed("Predicates","Predicates")
//                   .join(entities, col("ObjectID") === entities("ID"), "left_outer")
//                   .drop("ObjectID","ID")
//                   .withColumnRenamed("Entities","Objects")
      
      val result = dsTriplesInLong.withColumnRenamed(sub, "SubjectID")
                 .withColumnRenamed(pred,"PredicateID")
                 .withColumnRenamed(obj, "ObjectID")
                 .join(broadcast(entities), col("SubjectID") === entities("ID"), "left_outer")
                 .drop("SubjectID","ID")
                 .withColumnRenamed("Entities", "Subjects")
                 .join(broadcast(predicates), col("PredicateID") === predicates("ID") , "left_outer")
                 .drop("PredicateID","ID")
                 .withColumnRenamed("Predicates","Predicates")
                 .join(broadcast(entities), col("ObjectID") === entities("ID"), "left_outer")
                 .drop("ObjectID","ID")
                 .withColumnRenamed("Entities","Objects")

      return result.asInstanceOf[Dataset[RecordStringTriples]]
  }
  
  def getTriplesByString(dsTriplesInLong : Dataset[RecordLongTriples]) : Dataset[RecordStringTriples] = {

		 val entitiesMap = spark.sparkContext.broadcast(
           entities.collect().map{
              row => (row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[Long]).swap // swapping the order
           }.toMap
           )
           
		 val predicatesMap = spark.sparkContext.broadcast(
           predicates.collect().map{
              row => (row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[Long]).swap // swapping the order
           }.toMap
           )
           
     val result = dsTriplesInLong.mapPartitions{
                 iter => iter.map{
                             case trp : RecordLongTriples => RecordStringTriples(entitiesMap.value.get(trp.Subject).get,
                                                                                 predicatesMap.value.get(trp.Predicate).get,
                                                                                 entitiesMap.value.get(trp.Object).get )
                 }
     }
		 
    return result
  }
   
  def getEntitiesByIndex(dsEntitiesIndices : Dataset[Long]) : Dataset[(String,Long)] = {
    
    val colName = dsEntitiesIndices.columns(0)
    val result = dsEntitiesIndices.join(entities, dsEntitiesIndices(colName) === entities("ID"),"left_outer")
                           .select("Entities", "ID")
                           .asInstanceOf[Dataset[(String,Long)]]
                         
    return result
  }
  
  def getPredicatesByIndex(dsPredicatesIndicies : Dataset[Long]) : Dataset[(String,Long)] = {
    
		val colName = dsPredicatesIndicies.columns(0)
    val result = dsPredicatesIndicies.join(predicates, dsPredicatesIndicies(colName) === predicates("ID"),"left_outer")
                           .select("Predicates", "ID")
                           .asInstanceOf[Dataset[(String,Long)]]
                         
    return result
  }
  
  /**
   * The function returns all "entities" in their indexed form (Long)
   * and indices are consecutive. 
   * The column name is "ID".
   */
  def getEntities() : Dataset[Long] = {
    entities.select("ID").asInstanceOf[Dataset[Long]]
  }
  
   /**
   * The function returns all "predicates" in their indexed form (Long)
   * and indices are consecutive. 
   * The column name is "ID".
   */
  def getPredicates() : Dataset[Long] = {
    predicates.select("ID").asInstanceOf[Dataset[Long]]    
  }
  
  def getConvertedTriples() : Dataset[RecordLongTriples] = {
    getTriplesByIndex(triples.triples)
  }
    


}