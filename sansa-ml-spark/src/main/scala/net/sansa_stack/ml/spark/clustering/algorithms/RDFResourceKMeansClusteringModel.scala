package net.sansa_stack.ml.spark.clustering.algorithms

import org.apache.jena.graph.Triple
import org.apache.spark.ml.Model
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import net.sansa_stack.ml.spark.utils.RDF_Feature_Extractor

class RDFResourceKMeansClusteringModel(val uid: String, val sparkMLkMeansModel: KMeansModel)
  extends Model[RDFResourceKMeansClusteringModel] {

  override def copy(extra: ParamMap): RDFResourceKMeansClusteringModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val feature_extractor = new RDF_Feature_Extractor()
      .set_mode("at")
      .set_uri_key_column_name("uri")
      .set_features_column_name("fe_features")

    import net.sansa_stack.ml.spark.clustering.algorithms.RDFResourceKMeansClusterer.fromRow
    val triplesRDD: RDD[Triple] = dataset.rdd.map(row => fromRow(row.asInstanceOf[Row]))

    val featureDF: DataFrame = feature_extractor.transform(triplesRDD)

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("fe_features")
      .setOutputCol("features")
      .setVocabSize(1000000)
      .setMinDF(1)
      .fit(featureDF)

    val cvFeatures = cvModel
      .transform(featureDF)
      .select(col("uri"), col("features"))

    sparkMLkMeansModel.transform(cvFeatures)
  }

  override def transformSchema(schema: StructType): StructType =
    sparkMLkMeansModel.transformSchema(schema)

}
