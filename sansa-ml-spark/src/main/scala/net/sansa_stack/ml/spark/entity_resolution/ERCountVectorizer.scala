package net.sansa_stack.ml.spark.entity_resolution

import java.io.Serializable

import org.apache.jena.graph.Triple
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

class ERCountVectorizer(spark: SparkSession, sourceData1: RDD[Triple], sourceData2: RDD[Triple],
                        thresholdSubject: Double, jsimilarityPredicate: Double,
                        thresholdObject: Double, vocabSize: Long) extends Commons(spark, sourceData1, sourceData2,
  thresholdSubject, jsimilarityPredicate, thresholdObject, vocabSize) with Serializable {

  /**
   * This api vectroises the entity subjects tokenised to form features
   * Apply CountVectorizer vectorisation on the tokenised subjects, setting our setVocabSize means that in our dictionary we will be adding approximately terms<=vocab_size. Terms are in the inp_column
   * @param inp_col specifies the input column for vectorisation
   * @param out_col specifies the output column containing features
   * data1 and data2 are dataframes containing the tokenised subjects
   * @return Dataframes with vectorised features i.e. tokenised subjects are vectorised here
   */
  override def vectorise(inpCol: String, outCol: String, data1: DataFrame, data2: DataFrame): (DataFrame, DataFrame) = {
    val data = data1.union(data2).distinct()
    val countVectorizer = new CountVectorizer().setInputCol(inpCol).setOutputCol(outCol).setVocabSize(vocabSize.toInt).setMinDF(1).fit(data)
    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)
    val featuredEntitiesDf1 = countVectorizer.transform(data1).filter(isNoneZeroVector(col(outCol)))
    val featuredEntitiesDf2 = countVectorizer.transform(data2).filter(isNoneZeroVector(col(outCol)))
    return (featuredEntitiesDf1, featuredEntitiesDf2)
  }
}

object ERCountVectorizer {
  def apply(spark: SparkSession, sourceData1: RDD[Triple], sourceData2: RDD[Triple],
            thresholdSubject: Double, jsimilarityPredicate: Double,
            thresholdObject: Double, vocabSize: Long): ERCountVectorizer = new ERCountVectorizer(spark, sourceData1, sourceData2,
    thresholdSubject, jsimilarityPredicate, thresholdObject, vocabSize)
}
