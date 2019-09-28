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

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ConfBPairs(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}

object BigramPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfBPairs(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Frequency Count Pairs")
    val sc = new SparkContext(conf)
    val threshold = args.threshold()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
   
       val textFile = sc.textFile(args.input(), args.reducers())
   
   textFile
   .map(line => {
    var tokens = tokenize(line)
    if (tokens.length > 1)
     {
      var pairs = scala.collection.mutable.ListBuffer[(String)]()
        tokens.sliding(2).map(p => p.mkString(" ")).map(word => {
         var pair : (String) = word
         pairs += pair
        })
        tokens.map(p => p + " *").map(word => {
         var pair : (String) = word
         pairs += pair
        })
        pairs.toList
     }
    else List()
   })
   .map(pair => (pair,1))
   .reduceByKey(_+_)
   .sortByKey()
//    .collectAsMap()
   .map(pair => {
    var left = tokenize(pair._1.toString)(0)
    var right = tokenize(pair._1.toString)(1)
    var marginal = 0
    if(right == "*")
    {
     marginal = pair._2
     (pair._1, pair._2)
    }
    else
    {
     var sum = pair._2.toDouble / marginal.toDouble
     (pair._1, sum)
    }
   })
      .map(p => "(" + p._1._1 + ", " + p._1._2 + "), " + p._2)//    .sortByKey()
      .saveAsTextFile(args.output())
  }
}
    
