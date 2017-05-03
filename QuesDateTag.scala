// 11. Number of question with specific tags (nosql, big data) which was asked in the specified time range (from 01-01-2015 to 31-12-2015)
package com.df.stackoverflow
import java.text.SimpleDateFormat
import java.lang.String
import scala.xml.XML
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.functions.GroupReduceFunction

object QuesDateTag {
	def main(args: Array[String]) = {
	  	val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      val format2 = new SimpleDateFormat("yyyy-MM");
      val format3 = new SimpleDateFormat("yyyy-MM-dd");
      
      val startTime = format3.parse("2015-01-01").getTime
      val endTime = format3.parse("2015-01-31").getTime
      
		  val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
      val data = env.readTextFile("E:\\test\\Posts1.xml")

			val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.map {line => {
			  val xml = XML.loadString(line)
			  val crDate = xml.attribute("CreationDate").get.toString()
			  val tags = xml.attribute("Tags").get.toString()
//			  (closeDate, line)
			  (crDate, tags, line)
			  }
			}
			.filter{ data => {
			  var flag = false
			  val crTime = format.parse(data._1.toString()).getTime
			  if (crTime > startTime && crTime < endTime && (data._2.toLowerCase().contains("bigdata") || 
			      data._2.toLowerCase().contains("hadoop") || data._2.toLowerCase().contains("spark")))
			    flag = true
			  flag
			  }
			}
			
			result.print()
			println(result.count())
	}
}