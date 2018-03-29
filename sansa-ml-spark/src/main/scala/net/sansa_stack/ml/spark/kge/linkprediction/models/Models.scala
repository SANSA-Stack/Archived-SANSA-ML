package net.sansa_stack.ml.spark.kge.linkprediction.models

/**
 * Model Abstract Class
 * --------------------
 *
 * Created by lpfgarcia on 14/11/2017.
 */

import scala.math._
import scala.util._

import org.apache.spark.sql._

import com.intel.analytics.bigdl.nn.Power
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat

import net.sansa_stack.rdf.spark.kge.triples.{StringTriples,IntegerTriples}

abstract class Models(ne: Int, nr: Int, batch: Int, k: Int, sk: SparkSession) {

  val Ne = ne
  val Nr = nr

  var e = initialize(ne)
  var r = normalize(initialize(nr))

  def initialize(size: Int) = {
    Tensor(size, k).rand(-6 / sqrt(k), 6 / sqrt(k))
  }

  def normalize(data: Tensor[Float]) = {
    data / data.abs().sum()
  }

  import sk.implicits._

  val seed = new Random(System.currentTimeMillis())

  def tuple(aux: IntegerTriples) = {
    if (seed.nextBoolean()) {
      IntegerTriples(seed.nextInt(Ne) + 1, aux.Predicate, aux.Object)
    } else {
      IntegerTriples(aux.Subject, aux.Predicate, seed.nextInt(Ne) + 1)
    }
  }

  def negative(data: Dataset[IntegerTriples]) = {
    data.map(i => tuple(i))
  }

  def subset(data: Dataset[IntegerTriples]) = {
    data.sample(false, 2 * (batch.toDouble / data.count().toDouble)).limit(batch)
  }

  def L1(vec: Tensor[Float]) = {
    vec.abs().sum()
  }

  def L2(vec: Tensor[Float]) = {
    vec.pow(2).sqrt().sum()
  }

}