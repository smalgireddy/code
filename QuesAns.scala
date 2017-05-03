// 6. Number of questions with more than 2 answers
package com.df.stackoverflow

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool

import scala.xml.XML
import org.apache.flink.api.common.operators.Order

object QuesAns {
	def main(args: Array[String]) = {
	  
		  val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
      val data = env.readTextFile("E:\\test\\Posts1.xml")

			val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.map {line => {
			  val xml = XML.loadString(line)
			  (Integer.parseInt(xml.attribute("AnswerCount").getOrElse(0).toString()), line)
			  }
			}
			.filter{ x => { x._1 > 2 }
			}

			result.print()
			println(result.count())
	}
}
