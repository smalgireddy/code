//2. Most populous Countries - List of top 10 countries in the descending order of their population
package com.df.wbi

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import java.lang.Long
import org.apache.flink.api.common.operators.Order

object PopulousCountry {
	def main(args: Array[String]) = {
	  
		val env = ExecutionEnvironment.getExecutionEnvironment
		val data = env.readCsvFile[(String, String)]("E:\\test\\World_Bank_Indicators.csv", 
		    quoteCharacter='"', ignoreFirstLine=true, includedFields = Array(0, 9))

		val result = data.map( data => {
		  (data._1, Long.parseLong(data._2.replaceAll(",", "")), 0)
		})
    .groupBy(0)
    .max(1)
    .partitionByRange(2)
    .sortPartition(1, Order.DESCENDING)
    .map( data => (data._1, data._2))
			
			result.first(10).print()
			
	}
}