package net.sansa_stack.ml.spark.clustering.algorithms

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class ResourceClusteringModel extends Transformer {
  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = ???

  override val uid: String = _
}