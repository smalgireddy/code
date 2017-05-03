// 4. The trending questions which are scored highly by the user – Top 10 highest viewed questions with specific tags
package com.df.stackoverflow

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool

import scala.xml.XML
import org.apache.flink.api.common.operators.Order

object ScoreAnalysis {
	def main(args: Array[String]) = {
	  
		  val env = ExecutionEnvironment.getExecutionEnvironment  		// set up execution environment
      val data = env.readTextFile("E:\\test\\Posts1.xml")

    val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.map {line => {
			  val xml = XML.loadString(line)
			  (0, Integer.parseInt(xml.attribute("Score").getOrElse(0).toString()), line)
			  }
			}
			.partitionByRange(0)
			.sortPartition(1, Order.DESCENDING)
			.map ((line => (line._2, line._3)))
//			(7, <row Id="5" PostTypeId="1" CreationDate="2014-05-13T23:58:30.457" Score="7" ViewCount="286" Body="&lt;p&gt;I've always been interested in machine learning, but I can't figure out one thing about starting out with a simple &quot;Hello World&quot; example - how can I avoid hard-coding behavior?&lt;/p&gt;&#xA;&#xA;&lt;p&gt;For example, if I wanted to &quot;teach&quot; a bot how to avoid randomly placed obstacles, I couldn't just use relative motion, because the obstacles move around, but I don't want to hard code, say, distance, because that ruins the whole point of machine learning.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Obviously, randomly generating code would be impractical, so how could I do this?&lt;/p&gt;&#xA;" OwnerUserId="5" LastActivityDate="2014-05-14T00:36:31.077" Title="How can I do simple machine learning without hard-coding behavior?" Tags="&lt;machine-learning&gt;" AnswerCount="1" CommentCount="1" FavoriteCount="1" ClosedDate="2014-05-14T14:40:25.950" />)
//			.sortByKey(false)
			
			
//			result.first(50).print()
			result.print()

	}
}
