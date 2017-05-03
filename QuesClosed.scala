// 8. The distribution of number of questions closed per month
package com.df.stackoverflow
import java.text.SimpleDateFormat
import java.lang.String
import scala.xml.XML
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.functions.GroupReduceFunction

object QuesClosed {
	def main(args: Array[String]) = {
      val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      val format2 = new SimpleDateFormat("yyyy-MM");
      
		  val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
      val data = env.readTextFile("E:\\test\\Posts1.xml")

			val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.map {line => {
			  val xml = XML.loadString(line)

			  var closeDate = "";
			  if (xml.attribute("ClosedDate") != None)
			  {
			    val clDate = xml.attribute("ClosedDate").get.toString()
			    closeDate = format2.format(format.parse(clDate))
			  }
//			  (closeDate, line)
			  (closeDate, 1)
			  }
			}
			.filter{ data => {data._1.length() > 0}
			}
			.groupBy(0)
			.reduce( (a, b) => (a._1, a._2 + b._2 ))
			
			result.print()
	}
}