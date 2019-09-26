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

package io.bespin.scala.mapreduce.bigram

import io.bespin.scala.util.Tokenizer
import io.bespin.scala.util.WritableConversions

import java.util.StringTokenizer

import scala.collection.JavaConverters._
import scala.collection.mutable._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.apache.log4j._
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object BigramCount extends Configured with Tool with WritableConversions with Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  class MyMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
      val tokens = tokenize(value)
      if (tokens.length > 1)
        tokens.sliding(2).map(p => p.mkString(" ")).foreach(word => context.write(word, 1))
    }
  }

  class MyReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context) = {
      // Although it is possible to write the reducer in a functional style (e.g., with foldLeft),
      // an imperative implementation is clearer for two reasons:
      //
      //   (1) The MapReduce framework supplies an iterable over writable objects; since writable
      //       are container objects, it simply returns (a reference to) the same object each time
      //       but with a different payload inside it.
      //   (2) Implicit writable conversions in WritableConversions.
      //
      // The combination of both means that a functional implementation may have unpredictable
      // behavior when the two issues interact.
      var sum = 0
      for (value <- values.asScala) {
        sum += value
      }
      context.write(key, sum)
    }
  }

  override def run(argv: Array[String]) : Int = {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = getConf()
    val job = Job.getInstance(conf)

    FileInputFormat.addInputPath(job, new Path(args.input()))
    FileOutputFormat.setOutputPath(job, new Path(args.output()))

    job.setJobName("Word Count")
    job.setJarByClass(this.getClass)

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    job.setMapperClass(classOf[MyMapper])
    job.setCombinerClass(classOf[MyReducer])
    job.setReducerClass(classOf[MyReducer])

    job.setNumReduceTasks(args.reducers())

    val outputDir = new Path(args.output())
    FileSystem.get(conf).delete(outputDir, true)

    val startTime = System.currentTimeMillis()
    job.waitForCompletion(true)
    log.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds")

    return 0
  }

  def main(args: Array[String]) {
    ToolRunner.run(this, args)
  }
}
