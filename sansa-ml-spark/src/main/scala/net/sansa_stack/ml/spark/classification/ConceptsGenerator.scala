package net.sansa_stack.ml.spark.classification

import java.util.HashSet
import java.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.semanticweb.HermiT.Reasoner
import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLDataFactory
import org.semanticweb.owlapi.model.OWLIndividual
import org.semanticweb.owlapi.model.OWLNamedIndividual
//import net.sansa_stack.ml.spark.classification.KB

object ConceptsGenerator{
  class ConceptsGenerator(protected var kb: KB) {

    protected var reasoner: Reasoner = kb.getReasoner
    protected var dataFactory: OWLDataFactory = kb.getDataFactory
    protected var allExamples: RDD[OWLIndividual] = kb.getIndividuals
    var generator: Random = new Random(2)
    def generateQueryConcepts(numConceptsToGenerate: Int, sc: SparkSession): Array[OWLClassExpression] = {
      
      println("\nConcepts Generation\n-----------\n")
      val queryConcept: Array[OWLClassExpression] = Array.ofDim[OWLClassExpression](numConceptsToGenerate)
      val minOfSubConcepts: Int = 2
      val maxOfSubConcepts: Int = 8
      var numOfSubConcepts: Int = 0
      var i: Int = 0
      var j: Int = 0
      var nextConcept: OWLClassExpression = null
      var complPartialConcept: OWLClassExpression = null
      var nEx : Int = allExamples.count().toInt
      // cycle to build numConceptsToGenerate new query concepts
      i = 0
      while (i < numConceptsToGenerate) {
        var partialConcept: OWLClassExpression = null
        numOfSubConcepts = minOfSubConcepts + generator.nextInt(maxOfSubConcepts - minOfSubConcepts)
      
        var numPosInst: Int = 0
        var numNegInst: Int = 0
  
        // build a single new query OWLClassExpression adding conjuncts or disjuncts
        do {
  
          //take the first subConcept for builiding the query OWLClassExpression
          partialConcept = kb.getRandomConcept
          j = 1
                   
          while (j < numOfSubConcepts) {
            val newConcepts: HashSet[OWLClassExpression] = new HashSet[OWLClassExpression]()
            newConcepts.add(partialConcept)
            
            nextConcept = kb.getRandomConcept
            newConcepts.add(nextConcept)
           
            partialConcept =
              if (generator.nextInt(4) == 0)
                dataFactory.getOWLObjectIntersectionOf(newConcepts)
              else dataFactory.getOWLObjectUnionOf(newConcepts) 
            j+=1
          } // for j

          complPartialConcept = dataFactory.getOWLObjectComplementOf(partialConcept)
          //complPartialConcept = partialConcept.getComplementNNF 
          //println("\n", complPartialConcept)
          
          numPosInst = reasoner.getInstances(partialConcept, false).entities().count().toInt
          numNegInst = reasoner.getInstances(complPartialConcept, false).entities().count().toInt
          
          println("\n", partialConcept)
          
          println ("\n pos: " + numPosInst + ",  neg: " + numNegInst + ",  und: " + (nEx - numNegInst - numPosInst))
          println()

        } while ((numPosInst < 20) || (numNegInst > 3)) 
        
        // while ((numPosInst*numNegInst == 0))||((numPosInst<5)||(numNegInst<10))
        //while ((numPosInst < 20) || (numNegInst >3)) 
        //while (numPosInst+numNegInst == 0 || numPosInst+numNegInst == nEx)
        //while ((numPosInst < 20) || (numNegInst >3))     
   
          
        //add the newly built OWLClassExpression to the list of all required query concepts
        queryConcept(i) = partialConcept
        println("Query " + (i+1) + " found\n\n")
        i+=1
      }
  
     queryConcept
    }
  
  }
}