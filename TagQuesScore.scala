// 9. The most scored questions with specific tags – Questions having tag hadoop, spark in descending order of score
package com.df.stackoverflow

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool

import scala.xml.XML
import org.apache.flink.api.common.operators.Order

object TagQuesScore {
	def main(args: Array[String]) = {
	  
		  val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
      val data = env.readTextFile("E:\\test\\Posts1.xml")

						val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.map {line => {
			  val xml = XML.loadString(line)
			  (xml.attribute("Tags").get.toString(), Integer.parseInt(xml.attribute("Score").get.toString()), line)
//			  x,y,z
//			  tags, score, line
			  }
			}
			.filter { tag => {tag._1.contains("bigdata")}
			}
			.map { data => {
			  (0, data._2, data._3)
//			  score, line
			}
			}
			.partitionByRange(0)
			.sortPartition(1, Order.DESCENDING)

			result.print()
	}
}
