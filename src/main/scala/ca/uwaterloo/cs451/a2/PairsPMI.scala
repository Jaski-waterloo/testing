/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a2;

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._



class ConfPairs(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val window = opt[Int](descr = "cooccurrence window", required = false, default = Some(2))
  val threshold = opt[Int](descr = "threshold of output", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass.getName)
  
 def main(argv: Array[String])
 {
  
  val args = new ConfPairs(argv)

  log.info("Input: " + args.input())
  log.info("Output: " + args.output())
  log.info("Number of reducers: " + args.reducers())
  log.info("Threshold Value: " + args.threshold())
  val conf = new SparkConf().setAppName("PairsPMI")
  val sc = new SparkContext(conf)
  val outputDir = new Path(args.output())
  val threshold = args.threshold().toInt
  FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  val textFile = sc.textFile(args.input())
  
  val wordCount = textFile.flatMap(line => {
   val tokens = tokenize(line).take(40)
   val uniqueTokens = tokens.toSet
  uniqueTokens.map(t => t).toList
  })
   .map(word => (word,1)).reduceByKey(_+_)
  
  
  val totalLines = textFile.map(line => ("*", 1))
  .reduceByKey(_+_)
  
  val wordCountBroadcast = sc.broadcast(wordCount.collectAsMap())
  val totalLinesBroadcast = sc.broadcast(totalLines.lookup("*")(0))
  
  val pmi = textFile.flatMap(line => {
   val tokens = tokenize(line).take(40)
   val uniqueTokens = tokens.toSet
   var pairs = List[(String, String)]()
   for(x <- uniqueTokens)
   {
    for(y <- uniqueTokens)
    {
     if(x != y)
      pairs = pairs :+ (x,y)
    }
   }
   pairs.map(p => p._1.toString + " " + p._2).toList
  })
  .map(pair => ((tokenize(pair)(0), tokenize(pair)(1)), 1))
  .reduceByKey(_+_)
  .map(pair => {
   val left = wordCountBroadcast.value.get(pair._1._1).head
   val right = wordCountBroadcast.value.get(pair._1._2).head
   val both = pair._2
   
   if (both > threshold)
   {
    var fpmi = Math.log10(both.toDouble * totalLinesBroadcast.value / (left.toDouble * right.toDouble))
    (pair, (fpmi, both))
   }
  })
  .sortByKey()
  .saveAsTextFile(args.output())
  
  
 }

}
