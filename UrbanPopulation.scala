//1.Which country has the highest urban population
package com.df.wbi

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import java.lang.Long
import org.apache.flink.api.common.operators.Order

object UrbanPopulation {
	def main(args: Array[String]) = {

		val env = ExecutionEnvironment.getExecutionEnvironment
		
		val data = env.readCsvFile[(String, String)]("E:\\test\\World_Bank_Indicators.csv", 
		    quoteCharacter='"', ignoreFirstLine=true, includedFields = Array(0, 10))
//    val data = env.readCsvFile("", fieldDelimiter, quoteCharacter, ignoreFirstLine, ignoreComments, lenient, includedFields, pojoFields)

		val result = data.map( data => {
		  var uPop = 0L
		  if (data._2.length() > 0)
		    uPop = Long.parseLong(data._2.replaceAll(",", ""))
		  (data._1, uPop, 0)
		})
    .partitionByRange(2)
    .sortPartition(1, Order.DESCENDING)
    .map( data => (data._1, data._2))
			
			result.first(1).print()
	}
}