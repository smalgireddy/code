// 12. Average time for a post to get a correct answer
package com.df.stackoverflow
import java.text.SimpleDateFormat
import java.lang.String
import scala.xml.XML
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.functions.GroupReduceFunction

object AvgAnsTime {
	def main(args: Array[String]) = {
	  	val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      val format2 = new SimpleDateFormat("yyyy-MM");
      val format3 = new SimpleDateFormat("yyyy-MM-dd");
      
		  val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
      val data = env.readTextFile("E:\\test\\Posts1.xml")

			val baseData = data.filter{line => {line.trim().startsWith("<row")}
			}
			.map {line => {
			  val xml = XML.loadString(line)
			  var aaId = "";
			  if (xml.attribute("AcceptedAnswerId") != None)
			  {
			    aaId = xml.attribute("AcceptedAnswerId").get.toString()
			  }
			  val crDate = xml.attribute("CreationDate").get.toString()
			  val rId = xml.attribute("Id").get.toString()
//			  (closeDate, line)
			  (rId, aaId, crDate)
			  }
			}
			
			val aaData = baseData.map{ data => {
			  (data._2, data._3)
			}
			}
			.filter{ data => {data._1.length() > 0}}
			
			val rdata = baseData.map{ data => {
			  (data._1, data._3)
			}
			}
			val joinData = rdata.join(aaData).where(0).equalTo(0)
//			((10532,2016-03-04T19:17:18.713),(10532,2016-03-04T18:31:17.653))
			.map{ data => {
			  val quesDate = format.parse(data._2._2).getTime
			  val ansDate = format.parse(data._1._2).getTime
			  val diff : Float = ansDate - quesDate
			  val time : Float = diff/(1000 * 60 * 60)    //millisecond to hour
//			  (data, time)
			  time
			}
			}
			val count = joinData.count()
			val result = joinData.reduce((a,b) => a+b ).collect().head / count
			  
			println (result + " Hrs")
			
//			println(result.count())
	}
}