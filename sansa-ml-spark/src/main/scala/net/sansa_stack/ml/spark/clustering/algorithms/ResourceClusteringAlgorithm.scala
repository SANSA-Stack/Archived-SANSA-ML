package net.sansa_stack.ml.spark.clustering.algorithms

import org.apache.jena.ontology.OntTools.Path
import org.apache.jena.rdf.model.Property
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Clustering algorithm class for clustering RDF resources of a given dataset
 * of triples. In its default invocation it will use the RDF graph structure
 * to return clusters, or 'communities' as they are called in graph theory.
 * Furthermore one can define a set of properties or property paths that should
 * be used for clustering, e.g. a person's height and profession, or
 * a person's height and their mother's height (property path)
 *
 * One further setting that can be defined is the base clustering algorithm,
 * like k-means, DBSCAN etc.
 */
class ResourceClusteringAlgorithm extends Estimator[ResourceClusteringModel] {
  private val spark = SparkSession.builder().getOrCreate()

  private var properties = Set.empty[Property]
  private var propertyPaths = Set.empty[Path]

  def addClusteringProperties(properties: Set[Property]): Unit = {
    this.properties = properties

  }

  def addClusteringProperty(property: Property): Unit = {
    this.properties += property
  }

  def addClusteringPropertyPath(propertyPath: Path): Unit = {
    this.propertyPaths += propertyPath
  }

  def resetClusteringProperties: Unit = {
    this.properties = Set.empty[Property]
    this.propertyPaths = Set.empty[Path]
  }

  /*
   * TODO: Interface-wise it is not clear what to expect here. Dataset[Triple]? Dataset[(Node, Node, Node)]? Datset[(String, String, String)]?
   * TODO: Do we have an Encoder for org.apache.jena.graph.Triple or org.apache.jena.graph.Node objects somewhere in SANSA-RDF? If not we can work on Dataset[(String, String, String)] for now.
   */
  override def fit(dataset: Dataset[_]): ResourceClusteringModel = {
    import spark.implicits._
    dataset.as[(String, String, String)]
    // ...

    throw new NotImplementedError()
  }

  /*
   * TODO: Find out what this method is for
   */
  override def copy(extra: ParamMap): Estimator[ResourceClusteringModel] = defaultCopy(extra)

  /*
   * TODO: Find out what this method is for and how to implement it
   */
  override def transformSchema(schema: StructType): StructType = {
    throw new NotImplementedError()
  }

  /*
   * TODO: Find out what this is for and how to set it correctly
   */
  override val uid: String = "some id"
}


object ResourceClusteringAlgorithm {
  /* Supported base clustering algorithms */
  val KMEANS = "kmeans"
  val DBSCAN = "dbscan"
}