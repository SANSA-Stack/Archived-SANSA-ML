package net.sansa_stack.ml.spark.kge.linkprediction.NewDesign

object MyUtility {
  
	def printType[T](x: T): Unit = { println(x.getClass.toString()) }
	
	
	// http://biercoff.com/easily-measuring-code-execution-time-in-scala/
	def time[R](codeblock: => R): R = {  
    val t0 = System.currentTimeMillis
    val result = codeblock    // call-by-name
    val t1 = System.currentTimeMillis()
    println("\nElapsed time: " + (t1 - t0) + "ms, or " + (t1-t0)/1000.0 +"s. " )
    result
	}

	def exit = {
	  println(f"\nExiting(0) ...")
	  System.exit(0);
	}
}