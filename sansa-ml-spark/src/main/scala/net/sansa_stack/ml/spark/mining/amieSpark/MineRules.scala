package net.sansa_stack.ml.spark.mining.amieSpark

import java.io.File
import java.net.URI

import net.sansa_stack.ml.spark.mining.amieSpark.KBObject.KB
//import net.sansa_stack.ml.spark.mining.amieSpark.Rules.RuleContainer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SparkSession, _ }

import scala.collection.mutable.{ ArrayBuffer, Map }
import scala.util.Try

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import net.sansa_stack.ml.spark.mining.amieSpark.DfLoader.Atom

object MineRules {
  /**
   * 	Algorithm that mines the Rules.
   *
   * @param kb object knowledge base that was created in main
   * @param minHC threshold on head coverage
   * @param maxLen maximal rule length
   * @param threshold on confidence
   * @param sc spark context
   *
   *
   */
  
  case class RuleContainer (
      support: Option[Long], 
      parent: Option[RuleContainer], 
      rule: Option[ArrayBuffer[RDFTriple]], 
      sortedRule: Option[ArrayBuffer[RDFTriple]],
      bodySize: Option[Long],
      
      pcaBodySize: Option[Double], //0.0;
      pcaConfidence: Option[Double], //0.0;
      ancestors: Option[ArrayBuffer[(String, String, String)]],
      
      
      sizeHead: Option[Int]
      )
    
    
          //support: Option[Long], 
          //parent: Option[RuleContainer], 
          //rule: Option[ArrayBuffer[RDFTriple]], 
          //sortedRule: Option[ArrayBuffer[RDFTriple]],
          //bodySize: Option[Long],
          
          //pcaBodySize: Option[Double], //0.0;
          //pcaConfidence: Option[Double], //0.0;
          //ancestors: Option[ArrayBuffer[(String, String, String)]],
          
          
          //sizeHead: Option[Int]
    
    
    
    
  class Algorithm(k: KB, mHC: Double, mL: Int, mCon: Double, hdfsP: String) extends Serializable {

    val kb: KB = k
    val minHC = mHC
    val maxLen = mL
    val minConf = mCon
    val hdfsPath = hdfsP

    def calcName(whole: ArrayBuffer[RDFTriple]): String = {

      var countMap: Map[String, Int] = Map()
      var numberMap: Map[String, Int] = Map()
      var counter: Int = 1
      for (w <- whole) {
        if (countMap.contains(w._1)) {
          var temp = countMap.remove(w._1).get + 1
          countMap += (w._1 -> temp)
        } else {
          countMap += (w._1 -> 1)
        }
        if (countMap.contains(w._3)) {
          var temp = countMap.remove(w._3).get + 1
          countMap += (w._3 -> temp)
        } else {
          countMap += (w._3 -> 1)
        }
        if (!(numberMap.contains(w._1))) {
          numberMap += (w._1 -> counter)
          counter += 1
        }
        if (!(numberMap.contains(w._3))) {
          numberMap += (w._3 -> counter)
          counter += 1
        }

      }

      var out = ""
      for (wh <- whole) {
        var a = ""
        var b = ""
        if (countMap(wh._1) > 1) {
          a = numberMap(wh._1).toString
        } else {
          a = "0"
        }

        if (countMap(wh._3) > 1) {
          b = numberMap(wh._3).toString
        } else {
          b = "0"
        }

        out += a + "_" + wh._2 + "_" + b + "_"
      }
      out = out.stripSuffix("_")
      return out
    }

    case class QCaseClass (position: Int, ruleContainers: RuleContainer)
    
    
    def ruleMining(spark: SparkSession): ArrayBuffer[RuleContainer] = {

      var predicates = kb.getKbGraph().triples.map { x => x.predicate

      }.distinct
      //   var z = predicates.collect()

      /**
       * q is a queue with one atom rules
       * is initialized with every distinct relation as head, body is empty
       */
      //var q = ArrayBuffer[RuleContainer]()

      /*for (zz <- z) {
        if (zz != null) {
          var rule = ArrayBuffer(RDFTriple("?a", zz, "?b"))

          var rc = new RuleContainer
          rc.initRule(rule, kb, spark)

          q += rc
        }

      }*/
      
      println ("11111111")
       import spark.implicits._
      var count = 0
     var q:Dataset[QCaseClass] = predicates.map { p =>
        var rule = ArrayBuffer(RDFTriple("?a", p, "?b"))
        
      
       var y =  QCaseClass(count, initRule(rule, kb, spark))
      count+=1
       y
      }
      .toDS()
       println("222222")
      var outMap: Map[String, ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)]] = Map()

      var dataFrameRuleParts: RDD[(RDFTriple, Int, Int)] = null
      var out: ArrayBuffer[RuleContainer] = new ArrayBuffer
      var dublicate: ArrayBuffer[String] = ArrayBuffer("")

      for (i <- 0 to this.maxLen - 1) {

        if ((i > 0) && (dataFrameRuleParts != null)) {
          var temp = q
         q = null
         var qArr: Array[QCaseClass] =Array()
          // new ArrayBuffer

         
          
          var newAtoms1 = dataFrameRuleParts.collect
          
          var countQArr = 0
          
          for (n1 <- newAtoms1) {
            var parent: RuleContainer = RuleContainer(
               
                
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None)
            var newRuleC: RuleContainer =  RuleContainer(
              
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None)
            var parentDF = temp.filter($"position" === n1._3)//temp(n1._3)
            var newTpArrArr = parentDF.rdd.map{x => parent= x.ruleContainers
              var y = parent.rule.get
            y +=  n1._1
            }.collect//

             var newTpArr = newTpArrArr(0)
            var fstTp = newTpArr(0).toString()
            var counter = 1
            var sortedNewTpArr = new ArrayBuffer[RDFTriple]

            if (newTpArr.length > 2) {
              sortedNewTpArr = sort(newTpArr.clone)
            } else {
              sortedNewTpArr = newTpArr.clone
            }

            var dubCheck = fstTp

            for (i <- 1 to newTpArr.length - 1) {
              var temp = newTpArr(i).toString
              dubCheck += sortedNewTpArr(i).toString
              if (temp == fstTp) {
                counter += 1
              }
            }
            if ((counter < newTpArr.length) && (!(dublicate.contains(dubCheck)))) {
              dublicate += dubCheck
              newRuleC = setRule(newRuleC,minConf, n1._2, parent, newTpArr, sortedNewTpArr, kb, spark)
              qArr :+ QCaseClass(countQArr, newRuleC)
              countQArr += 1
            }

          }
          
          q = qArr.toSeq.toDS

        } else if ((i > 0) && ((dataFrameRuleParts == null) || (dataFrameRuleParts.isEmpty()))) {
          q = null
        }

        if (q != null) {
           
           q.rdd.map{ qElem =>

            val r: RuleContainer = qElem.ruleContainers

            var tp = r.rule.get
            if (tp.length > 2) {
              tp = r.sortedRule.get

            }

            if (acceptedForOutput(outMap, r, minConf, kb, spark)) {
              out += r

              if (!(outMap.contains(tp(0).predicate))) {
                outMap += (tp(0).predicate -> ArrayBuffer((tp, r)))
              } else {
                var temp: ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)] = outMap.remove(tp(0).predicate).get
                temp += new Tuple2(tp, r)
                outMap += (tp(0).predicate -> temp)

              }

            }
            var R = new ArrayBuffer[RuleContainer]()

            if (r.rule.get.length < maxLen) {

              dataFrameRuleParts = refine(i, qElem.position, r, dataFrameRuleParts, spark)
              //TODO: Dublicate check

            }

          }
        }

      }

      return out
    }

    /**
     * exploring the search space by iteratively extending rules using a set of mining operators:
     * - add dangling atom
     * - add instantiated atom
     * - add closing atom
     *
     */

    def refine(c: Int, id: Int, r: RuleContainer, dataFrameRuleParts: RDD[(RDFTriple, Int, Int)], spark: SparkSession): RDD[(RDFTriple, Int, Int)] = {

      var out: DataFrame = null
      var OUT: RDD[(RDFTriple, Int, Int)] = dataFrameRuleParts
      //var count2:RDD[(String, Int)] = null 
      var path = new File("test_table/")
      var temp = 0

      val tpAr = r.rule.get

      var stringSELECT = ""
      for (tp <- 0 to tpAr.length - 1) {

        stringSELECT += "tp" + tp + ", "

      }

      stringSELECT += "tp" + tpAr.length

      var z: Try[Row] = null
      if ((tpAr.length != maxLen - 1) && (temp == 0)) {
        var a = kb.addDanglingAtom(c, id, minHC, r, spark)

        z = Try(a.first())
        if ((!(z.isFailure)) && (z.isSuccess)) {

          out = a

        }

      }

      var b = kb.addClosingAtom(c, id, minHC, r, spark)

      var t = Try(b.first)

      if ((!(t.isFailure)) && (t.isSuccess) && (temp == 0)) {

        if (out == null) {
          out = b
        } else {
          out = out.unionAll(b)

        }

      }

      var count: RDD[(String, Int)] = null
      var o: RDD[(RDFTriple, Int, Int)] = null

      if (((!(t.isFailure)) && (t.isSuccess)) || ((z != null) && (!(z.isFailure)) && (z.isSuccess))) {
        count = out.rdd.map(x => (x(r.rule.get.length + 1).toString(), 1)).reduceByKey(_ + _)

        o = count.map(q => (q._1.split("\\s+"), q._2)).map { token =>
          Tuple3(RDFTriple(token._1(0), token._1(1), token._1(2)), token._2, token._1(3).toInt)
        }.filter(n1 => (n1._2 >= (kb.getRngSize(n1._1.predicate) * minHC)))

        if (OUT == null) {
          OUT = o
        } else {
          OUT = OUT.union(o)
        }

      }

      return OUT

    }

    /**
     * checks if rule is a useful output
     *
     * @param out output
     * @param r rule
     * @param minConf min. confidence
     *
     */
    def acceptedForOutput(outMap: Map[String, ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)]], r: RuleContainer, minConf: Double, k: KB, spark: SparkSession): Boolean = {

      //if ((!(r.closed())) || (r.getPcaConfidence(k, sc, sqlContext) < minConf)) {
      if ((!(closed(r))) || (r.pcaConfidence.get < minConf)) {
        return false

      }

      var parents: ArrayBuffer[RuleContainer] = parentsOfRule(r,outMap, spark)
      if (r.rule.get.length > 2) {
        for (rp <- parents) {
          if (r.pcaConfidence.get <= rp.pcaConfidence.get) {
            return false
          }

        }
      }

      return true
    }

    def sort(tp: ArrayBuffer[RDFTriple]): ArrayBuffer[RDFTriple] = {
      var out = ArrayBuffer(tp(0))
      var temp = new ArrayBuffer[Tuple2[String, RDFTriple]]

      for (i <- 1 to tp.length - 1) {
        var tempString: String = tp(i).predicate + tp(i).subject + tp(i).`object`
        temp += Tuple2(tempString, tp(i))

      }
      temp = temp.sortBy(_._1)
      for (t <- temp) {
        out += t._2
      }

      return out
    }

  }

  
  
  
  
  //TODO: change all RuleContainers to case class
  
      def initRule( x: ArrayBuffer[RDFTriple], k: KB, spark: SparkSession): RuleContainer = {
      

      var supp = calcSupport(x ,k, spark)
      var bodSize = setBodySize(x, k, spark)
      var hs = setSizeHead(x, k)
      
     return RuleContainer( 
         supp, 
         None,
         Some(x), 
         None,
         bodSize,
         
         None, //pcaBodySize: Option[Double], //0.0;
         None, // pcaConfidence: Option[Double], //0.0;
   
         None, // ancestors: Option[ArrayBuffer[(String, String, String)]],
         
        
         None//sizeHead: Option[Int]
    )

    }
      
     
  
  
    def setRule(rc: RuleContainer, threshold: Double, supp: Long, p: RuleContainer, x: ArrayBuffer[RDFTriple], y: ArrayBuffer[RDFTriple], k: KB, spark: SparkSession): RuleContainer = {
      

      

      

     
      

      

     
      
      return RuleContainer (
      Some(supp),    //support: Option[Long], 
      Some(p),    //parent: Option[RuleContainer], 
      Some(x),    //rule: Option[ArrayBuffer[RDFTriple]], 
      Some(y),   //sortedRule: Option[ArrayBuffer[RDFTriple]],
      setBodySize(x, k, spark),    //bodySize: Option[Long],
      
      setPcaConfidence(x, supp, threshold, k, spark),    //pcaBodySize: Option[Double], //0.0;
      None,    //pcaConfidence: Option[Double], //0.0;
      None,    //ancestors: Option[ArrayBuffer[(String, String, String)]],
      
      
      Some(setSizeHead(x, k))    //sizeHead: Option[Int]
          )
    }

   

    /**returns ArrayBuffer with every triplePattern of the body as a RDFTriple*/

    def hc(rc: RuleContainer): Double = {
      if (rc.bodySize.get < 1) {
        return -1.0

      }

      return ((rc.support.get) / (rc.sizeHead.get))

    }

    /**
     * returns number of instantiations of the predicate of the head
     *
     * @param k knowledge base
     *
     */
    def setSizeHead(rule: ArrayBuffer[RDFTriple], k: KB): Int = {
    
      var headElem = rule(0)

      var rel = headElem.predicate

      return k.sizeOfRelation(rel)

     
    }

    /**
     * returns number of instantiations of the relations in the body
     *
     * @param k knowledge base
     * @param sc spark context
     *
     */

    def setBodySize(rule:ArrayBuffer[RDFTriple], k: KB, spark: SparkSession): Option[Long]= {
      if ((rule.length - 1) > 1) {
        var body = rule.clone
        body.remove(0)
        var mapList = k.cardinality(body, spark)

       return Some(mapList.count())
      }
      return Some(-1)
    }

    /**
     * returns the support of a rule, the number of instantiations of the rule
     *
     * @param k knowledge base
     * @param sc spark context
     *
     */

    def calcSupport(rule: ArrayBuffer[RDFTriple], k: KB, spark: SparkSession): Option[Long] = {

      
      
      if (rule.length > 1) {

        val mapList = k.cardinality(rule, spark)

        return Some(mapList.count())
      }
      return Some(-1)
    }
    /**returns the length of the body*/

  

    def usePcaApprox(tparr: ArrayBuffer[RDFTriple]): Boolean = {

      var maptp: Map[String, Int] = Map()
      if (tparr.length <= 2) {
        return false
      }

      for (x <- tparr) {

        if (!(maptp.contains(x._1))) {
          maptp += (x._1 -> 1)
        } else {
          var counter: Int = maptp.remove(x._1).get + 1
          if (counter > 2) { return false }

          maptp += (x._1 -> counter)
        }

        /**checking map for placeholder for the object*/
        if (!(maptp.contains(x._3))) {
          maptp += (x._3 -> 1)
        } else {
          var counter: Int = maptp.remove(x._3).get + 1
          if (counter > 2) { return false }

          maptp += (x._3 -> counter)
        }

      }
      var counter = 0
      var isLength = false
      maptp.foreach { x =>
        counter += 1

        if (x._2 == tparr.length) {
          isLength = true
        }
      }
      if ((isLength) && (counter == 2)) {
        return false
      }
      var max = 0
      maptp.foreach { value =>
        if (value._2 == 1) { return false }
        if (max < value._2) { max = value._2 }
      }
      if (max > 2) {
        return false
      }
      return true

    }

    def setPcaConfidence(rule: ArrayBuffer[RDFTriple], supp: Long, threshold: Double, k: KB, spark: SparkSession): Option[Double] = {
     
      var tparr = rule.clone
      val usePcaA = usePcaApprox(tparr)

      /**
       * when it is expensive to compute pca approximate pca
       */
      if (usePcaA) {

        
        if (pcaConfidenceEstimation(rule, k) > threshold) {

          var pcaBodySize = k.cardPlusnegativeExamplesLength(tparr, supp, spark)

          if (pcaBodySize > 0.0) {
            return Some(( supp/ pcaBodySize))
          }
        }

      } else {

        var pcaBodySize = k.cardPlusnegativeExamplesLength(tparr, supp, spark)
        if (pcaBodySize > 0.0) {
         return Some((supp / pcaBodySize))
        }

      }
      return Some(0.0)

    }

   
/*
    def setPcaBodySize(k: KB, spark: SparkSession) {
      val tparr = this.rule

      val out = k.cardPlusnegativeExamplesLength(tparr, spark)

      this.pcaBodySize = out

    }
    * 
    */

    def pcaConfidenceEstimation(rule: ArrayBuffer[RDFTriple], k: KB): Double = {
      var r = rule.clone
      var r0 = (k.overlap(r(1).predicate, r(0).predicate, 0)) / (k.functionality(r(1).predicate))
      var rrest: Double = 1.0

      for (i <- 2 to r.length - 1) {
        rrest = rrest * (((k.overlap(r(i - 1).predicate, r(i).predicate, 2)) / (k.getRngSize(r(i - 1).predicate))) * ((k.inverseFunctionality(r(i).predicate)) / (k.functionality(r(i).predicate))))

      }

     return (r0 * rrest)

    }

    def parentsOfRule(rc: RuleContainer, outMap: Map[String, ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)]], spark: SparkSession): ArrayBuffer[RuleContainer] = {
      // TODO: create new rules with body in alphabetical order     
      var parents = ArrayBuffer(rc.parent.get)
      val r = rc.sortedRule.get.clone

      if (outMap.get(r(0).predicate) == None) { return parents }
      var rel = outMap.get(r(0).predicate).get

      var tp: ArrayBuffer[RDFTriple] = new ArrayBuffer

      var filtered = rel.filter(x => (x._1.length == r.length - 1))

      /* 
             for (f <- filtered){
               var bool = true
               for (ff <- f._1){
                 if (!(r.contains(ff))){
                   bool = false
                 }
               }
               if (bool){
                 parents += f._2
               }
             }*/

      for (l <- 1 to r.length - 1) {
        if (!(filtered.isEmpty)) {

          tp = r.clone()
          tp.remove(l)
          var x = partParents(tp, filtered, spark)
          parents ++= x._1
          filtered = x._2
        }
      }

      return parents

    }

    def closed(rc: RuleContainer): Boolean = {

      var tparr = rc.rule.get
      var maptp: Map[String, Int] = Map()
      if (tparr.length == 1) {
        return false
      }

      for (x <- tparr) {

        if (!(maptp.contains(x._1))) {

          maptp += (x._1 -> 1)
        } else {

          maptp.put(x._1, (maptp.get(x._1).get + 1)).get
        }

        /**checking map for placeholder for the object*/
        if (!(maptp.contains(x._3))) {

          maptp += (x._3 -> 1)
        } else {
          maptp.put(x._3, (maptp.get(x._3).get + 1)).get
        }

      }

      maptp.foreach { value => if (value._2 == 1) return false }

      return true

    }

    

    

   

    def partParents(triples: ArrayBuffer[RDFTriple], arbuf: ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)], spark: SparkSession): (ArrayBuffer[RuleContainer], ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)]) = {
      var out1: ArrayBuffer[RuleContainer] = new ArrayBuffer
      var out2: ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)] = new ArrayBuffer

      var out: (ArrayBuffer[RuleContainer], ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)]) = (out1, out2)
      if (triples.length <= 1) {
        return out
      }
      var triplesCardcombis: ArrayBuffer[ArrayBuffer[RDFTriple]] = new ArrayBuffer

      //var rdd =sc.parallelize(arbuf.toSeq)
      //out ++= rdd.filter(x => (sameRule(triples, x._1))).map(y => y._2).collect

      for (x <- arbuf) {
        if (sameRule(triples, x._1)) {
          out1 += x._2

        } else out2 += x
      }

      /* var rdd = sc.parallelize(arbuf.toSeq)
          var pq = rdd.map{ x=>
           if (sameRule(triples, x._1)){
             ("out1", x._2) 
             
           }
           else ("out2",x)
         }.groupByKey()
         * 
         */

      out = (out1, out2)
      return out

    }

    def rdTpEquals(a: RDFTriple, b: RDFTriple): Boolean = {
      if ((a._1 == b._1) && (a._2 == b._2) && (a._3 == b._3)) {
        return true

      } else {
        return false
      }
    }

    def rdfTpArrEquals(a: ArrayBuffer[RDFTriple], b: ArrayBuffer[RDFTriple]): Boolean = {
      if (!(a.length == b.length)) {
        return false
      }

      for (i <- 0 to a.length - 1) {
        if (!(rdTpEquals(a(i), b(i)))) {
          return false

        }
      }
      return true
    }

    def sameRule(a: ArrayBuffer[RDFTriple], b: ArrayBuffer[RDFTriple]): Boolean = {
      var varMap: Map[String, String] = Map()
      if (a.length != b.length) { return false }
      for (i <- 0 to a.length - 1) {

        if (a(i)._2 != b(i)._2) { return false }
        if (!(varMap.contains(a(i)._1))) {
          varMap += (a(i)._1 -> b(i)._1)
        }

        if (!(varMap.contains(a(i)._3))) {
          varMap += (a(i)._3 -> b(i)._3)
        }

        if (!((varMap.get(a(i)._1) == Some(b(i)._1)) && (varMap.get(a(i)._3) == Some(b(i)._3)))) {
          return false
        }

      }

      return true
    }

    def sort(tp: ArrayBuffer[RDFTriple]): ArrayBuffer[RDFTriple] = {
      var out = ArrayBuffer(tp(0))
      var temp = new ArrayBuffer[Tuple2[String, RDFTriple]]

      for (i <- 1 to tp.length - 1) {
        var tempString: String = tp(i).predicate + tp(i).subject + tp(i).`object`
        temp += Tuple2(tempString, tp(i))

      }
      temp = temp.sortBy(_._1)
      for (t <- temp) {
        out += t._2
      }

      return out
    }

  
  
  
  
  
  
  
  
  def main(args: Array[String]) = {
    val know = new KB()

    val spark = SparkSession.builder

      .master("local[*]")
      .appName("AMIESpark example")

      .getOrCreate()

    if (args.length < 2) {
      System.err.println(
        "Usage: Triple reader <input> <output>")
      System.exit(1)
    }

    val input = args(0)
    val outputPath: String = args(1)
    val hdfsPath: String = outputPath + "/"

    know.sethdfsPath(hdfsPath)
    know.setKbSrc(input)

    know.setKbGraph(RDFGraphLoader.loadFromFile(know.getKbSrc(), spark, 2))
    know.setDFTable(DfLoader.loadFromFileDF(know.getKbSrc, spark, 2))

    val algo = new Algorithm(know, 0.01, 3, 0.1, hdfsPath)

    var output = algo.ruleMining(spark)

    var outString = output.map { x =>
      var rdfTrp = x.rule.get
      var temp = ""
      for (i <- 0 to rdfTrp.length - 1) {
        if (i == 0) {
          temp = rdfTrp(i) + " <= "
        } else {
          temp += rdfTrp(i) + " \u2227 "
        }
      }
      temp = temp.stripSuffix(" \u2227 ")
      temp
    }.toSeq
    var rddOut = spark.sparkContext.parallelize(outString)

    rddOut.saveAsTextFile(outputPath + "/testOut")

    spark.stop

  }

}