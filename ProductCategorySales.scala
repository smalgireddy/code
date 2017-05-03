//1. Calculate sales breakdown by product category across all of the stores.
package com.df.ra

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import java.lang.Float

object ProductCategorySales {
	def main(args: Array[String]) {

		val params: ParameterTool = ParameterTool.fromArgs(args)
		val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
		env.getConfig.setGlobalJobParameters(params)          		  // make parameters available in the web interface
    val data = env.readTextFile(params.get("input"))

		val result = data.map { line => {
  		val tokens = line.split("\\t")
		  (tokens(3), Float.parseFloat(tokens(4)))
		}}
//		Prod-Cat  Sale-Val
//		Women's Clothing   153.57
//		Women's Clothing	 483.82
		.groupBy(0)
		.sum(1)

    result.writeAsCsv(params.get("output"), "\n", ",")
//    counts.writeAsText("output")
    
    env.execute("Scala WordCount Example")
	}
}
// bin/flink run --class com.df.ra.ProductCategorySales ../ra-flink-sjob.jar -input ../Retail_Sample_Data_Set.txt -output rda-prod-out-02
