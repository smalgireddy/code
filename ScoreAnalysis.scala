// 3. Provide the number of posts which are questions and contains specified words in their title (like data, science, nosql, hadoop, spark)
package com.df.stackoverflow

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool

import scala.xml.XML

object TitleAnalysis {
	def main(args: Array[String]) = {
	  
		  val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
      val data = env.readTextFile("E:\\test\\Posts1.xml")

			val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.flatMap {line => {
			  val xml = XML.loadString(line)
			  xml.attribute("Title")
			  }
			}
			.filter { line => {
			  line.mkString.toLowerCase().contains("hadoop")
			}
			}
			
			result.print()
			println("Total Count: " + result.count())

	}
}
