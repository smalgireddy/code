//3. Find the total sales values across all the stores and the total number of sales.
package com.df.ra

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import java.lang.Float

object TotalSales {
	def main(args: Array[String]) {

		val params: ParameterTool = ParameterTool.fromArgs(args)
		val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
		env.getConfig.setGlobalJobParameters(params)          		  // make parameters available in the web interface
    val data = env.readTextFile(params.get("input"))
//		Date Time City Product-Cat Sale-Value Payment-Mode
//		2012-01-01 09:00 Fort Worth Women's Clothing 153.57 Visa

		val result = data.map { line => {
  		val tokens = line.split("\\t")
		  ("Total Sales", Float.parseFloat(tokens(4)))
		}}
		.groupBy(0)
		.sum(1)

    result.writeAsCsv(params.get("output"), "\n", ",")
//    counts.writeAsText("output")
    
    env.execute("Scala WordCount Example")
	}
}
// bin/flink run --class com.df.ra.TotalSales ../ra-flink-sjob.jar -input ../Retail_Sample_Data_Set.txt -output rda-total-out-02
