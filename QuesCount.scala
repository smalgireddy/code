//1. Count the total number of questions in the available data-set and collect the questions id of all the questions
package com.df.stackoverflow

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool

object QuesCount {
	def main(args: Array[String]) = {
	  
		  val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
      val data = env.readTextFile("E:\\test\\Posts1.xml")

			val result = data.filter{line => {line.trim().startsWith("<row")}			   
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			
			result.print()
			println("Total Count: " + result.count())

	}
}
