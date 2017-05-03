// 10. List of all the tags along with their counts
package com.df.stackoverflow

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import scala.xml.XML
object TagAnalysis {
	def main(args: Array[String]) = {
	  
		  val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
      val data = env.readTextFile("E:\\test\\Posts1.xml")

			val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.map {line => {
//			  line
			  val xml = XML.loadString(line)
			  xml.attribute("Tags").get.toString()
//			  tagString
			  }
			}
			.flatMap { data => {
//			  tagString
			  data.replaceAll("&lt;", " ").replaceAll("&gt;", " ").split(" ")
//			  individual tag like spark
			}
			}
			.filter { tag => {tag.length() > 0 }
			}
			.map { data => {
			  (1, data)
			}
			}
			.groupBy(1)
			.sum(0)

			result.print()
	}
}