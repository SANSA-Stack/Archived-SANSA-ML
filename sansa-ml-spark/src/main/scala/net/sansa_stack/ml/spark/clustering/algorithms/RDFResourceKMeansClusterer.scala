package net.sansa_stack.ml.spark.clustering.algorithms

import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.util.NodeFactoryExtra
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import net.sansa_stack.ml.spark.utils.RDF_Feature_Extractor

class RDFResourceKMeansClusterer(override val uid: String) extends Estimator[RDFResourceKMeansClusteringModel] {

  private var k: Int = 2

  def setK(k: Int): Unit = {
    this.k = k
  }

  override def fit(dataset: Dataset[_]): RDFResourceKMeansClusteringModel = {
    val feature_extractor = new RDF_Feature_Extractor()
      .set_mode("at")
      .set_uri_key_column_name("uri")
      .set_features_column_name("fe_features")

    val triplesRDD: RDD[Triple] = dataset.rdd.map(
      row => RDFResourceKMeansClusterer.fromRow(row.asInstanceOf[Row]))
    val featureDF: DataFrame = feature_extractor.transform(triplesRDD)

//    featureDF.show(100, false)

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("fe_features")
      .setOutputCol("features")
      .setVocabSize(1000000)
      .setMinDF(1)
      .fit(featureDF)

    val cvFeatures = cvModel
      .transform(featureDF)
      .select(col("uri"), col("features"))

//    cvFeatures.show(100, false)

    val mlLibClusterer = new KMeans("some uid")
    mlLibClusterer.setK(k)

    val sparkMLModel = mlLibClusterer.fit(cvFeatures)

    // FIXME: provide an actual UID
    val model = new RDFResourceKMeansClusteringModel("some uid", sparkMLModel)

    model
  }

  override def copy(extra: ParamMap): Estimator[RDFResourceKMeansClusteringModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    throw new NotImplementedError()
  }
}

object RDFResourceKMeansClusterer {
  def main(args: Array[String]): Unit = {
    import net.sansa_stack.rdf.spark.io._

    val spark = SparkSession.builder()
      .appName("Foo")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val inputFilePath = args(0)
    val inputDF: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputFilePath)

    val clusteringModel: RDFResourceKMeansClusteringModel =
      new RDFResourceKMeansClusterer("some_uid").fit(inputDF)

    val clusters = clusteringModel.transform(inputDF)

    clusters.drop("features").show(100, false)
  }

  /**
    * This is a code copy of the function net.sansa_stack.rdf.spark.io.fromRow
    * which seemingly has a bug as reported here:
    * https://github.com/SANSA-Stack/SANSA-RDF/issues/99
    *
    * @param row A triple encoded as a Row object
    * @return The Apache Jena Triple object representation of the Row
    *         representation triple
    */
  def fromRow(row: Row): org.apache.jena.graph.Triple = {
    val sStr = row.getString(0)

    val s =
      if (sStr.startsWith("_:")) NodeFactory.createBlankNode(sStr)
      else NodeFactoryExtra.parseNode(s"<$sStr>")

    val p = NodeFactoryExtra.parseNode("<" + row.getString(1) + ">")

    val oStr = row.getString(2)
    val o = if (oStr.startsWith("_:")) {
      NodeFactory.createBlankNode(oStr)
    } else if (oStr.startsWith("http") && !oStr.contains("^^")) {
      NodeFactory.createURI(oStr)
    } else {
      var lit = oStr
      val idx = oStr.indexOf("^^")
      if (idx > 0) {
        val first = oStr.substring(0, idx)
        val second = oStr.substring(idx + 2).trim
        lit = first + "^^" + second
      }
      NodeFactoryExtra.parseNode(lit)
    }

    Triple.create(s, p, o)
  }
}