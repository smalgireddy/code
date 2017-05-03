// 7. Number of questions which are active for more than 6 months
package com.df.stackoverflow
import java.text.SimpleDateFormat
import java.lang.String
import scala.xml.XML
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.functions.GroupReduceFunction

object QuesActive {
	def main(args: Array[String]) = {
	  	val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      
		  val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
      val data = env.readTextFile("E:\\test\\Posts1.xml")

			val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.map {line => {
			  val xml = XML.loadString(line)
			  (xml.attribute("CreationDate").get,  xml.attribute("LastActivityDate").get, line)
//			  data._1                              data._2                              data._3
			  }
			}
			.map{ data => {
			  val crDate = format.parse(data._1.text)
			  val crTime = crDate.getTime;
			  
			  val edDate = format.parse(data._2.text)
			  val edTime = edDate.getTime;
			  
			  val timeDiff : Long = edTime - crTime
			  (crDate, edDate, timeDiff, data._3)
//			 data._1 data._2 data._3 data._4
			}
			}
			.filter { data => { data._3 / (1000 * 60 * 60 * 24) > 30*6}
			}
			
			result.print()
			println(result.count())
	}
}