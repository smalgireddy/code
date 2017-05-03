// 2. Monthly questions count – provide the distribution of number of questions asked per month
package com.df.stackoverflow
import java.text.SimpleDateFormat
import java.lang.String
import scala.xml.XML
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.functions.GroupReduceFunction

object QuesAnalysis {
	def main(args: Array[String]) = {
	  	val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      val format2 = new SimpleDateFormat("yyyy-MM");
      
		  val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
      val data = env.readTextFile("E:\\test\\Posts1.xml")

			val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.flatMap {line => {
			  val xml = XML.loadString(line)
			  xml.attribute("CreationDate")
			  }
			}
			.map { line => {
        (format2.format(format.parse(line.toString())).toString(), 1)
			}
			}
			.groupBy(0)
//			.reduce( (a, b) => (a._1, a._2 + b._2 ))
			.reduceGroup( element => {
			  var sum = 0
			  var key = "";
			  while (element.hasNext)
			  {
			    val ele = element.next()
			    if (sum == 0)
			      key = ele._1
			    sum += ele._2
			  }
			  (key, sum)
			})
//      .sum(1)
			
			result.print()

	}
//	class QuesCounter extends GroupReduceFunction[Tuple2, Triad] {
	  
//	}
//	case class Edge(v1: Int, v2: Int) extends Serializable

}
