package net.sansa_stack.ml.spark.mining.amieSpark

import net.sansa_stack.ml.spark.mining.amieSpark._

import org.apache.spark.SparkConf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import KBObject.KB
import scala.collection.mutable.Map

object Rules {

  class RuleContainer() extends Serializable {
    val name: String = ""

    var rule: ArrayBuffer[RDFTriple] = new ArrayBuffer
    var sortedRule: ArrayBuffer[RDFTriple] = new ArrayBuffer

    var headKey = null;
    var support: Long = -1;
    var hashSupport = 0;
    var parent: RuleContainer = null;
    var bodySize = -1.0;
    var highestVariable: Char = 'b';
    var pcaBodySize = 0.0;
    var pcaConfidence = 0.0;
    var _stdConfidenceUpperBound = 0.0;
    var _pcaConfidenceUpperBound = 0.0;
    var _pcaConfidenceEstimation = 0.0;
    var _belowMinimumSupport = false;
    var _containsQuasiBindings = false;
    var ancestors = new ArrayBuffer[(String, String, String)]()
    var highestVariableSuffix = 0;
    var variableList: ArrayBuffer[String] = ArrayBuffer("?a", "?b")
    var sizeHead: Double = 0.0

    def getRule(): ArrayBuffer[RDFTriple] = {
      return this.rule
    }

    def getSortedRule(): ArrayBuffer[RDFTriple] = {
      return this.sortedRule
    }

    /**initializes rule, support, bodySize and sizeHead*/


    //end

  }

}