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

class ConfP(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold value", required = false, default = Some(10))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfP(argv)

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
    
    val single_counts = textFile.flatMap( line => {
            val tokens = tokenize(line).take(40)
            val set_tokens = tokens.toSet
            set_tokens.map( p => p).toList
       })
       .map(gram => (gram, 1))
       .reduceByKey(_+_)
    
    val total_count = textFile.flatMap( line => {
            val tokens = tokenize(line).take(40)
            val set_tokens = tokens.toSet
            set_tokens.map( p => p).toList
       })
       .map(gram => ("*",1))
       .reduceByKey(_+_)
       
    val total_count_B = sc.broadcast(total_count.lookup("*")(0))
    val single_counts_B = sc.broadcast(single_counts.collectAsMap())
      
    val pmi = textFile.flatMap( line => {
            val tokens = tokenize(line)
            val set_tokens = tokens.take(40).toSet
            var pairs = List[(String,String)]()
            for (x<-set_tokens) {
              for (y<-set_tokens) {
                if (x != y) {
                  pairs = pairs :+ (x,y)
                }
              }
            }
            pairs.map(p=> p._1.toString + " " + p._2  ).toList 
       })
       .map(bigram => ((tokenize(bigram)(0),tokenize(bigram)(1)), 1))
       .reduceByKey(_+_)
       .map(gram => {
            val joint_count = gram._2
            val prob_A = single_counts_B.value.get(gram._1._1).head
            val prob_B = single_counts_B.value.get(gram._1._2).head
            if (joint_count > threshold) {
                val pmi = Math.log10(joint_count.toDouble*total_count_B.value/(prob_A.toDouble*prob_B.toDouble))
                ((gram._1),(pmi,joint_count))
            }
            else (("Below","Threshold"),(0,0))
       })
       .sortByKey()
    pmi.saveAsTextFile(args.output())
  }
}
