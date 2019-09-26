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
import io.bespin.scala.util.WritableConversions

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
import tl.lin.data.pair.PairOfStrings
import tl.lin.data.pair.PairOfIntFloat

class ConfPairs(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val window = opt[Int](descr = "cooccurrence window", required = false, default = Some(2))
  verify()
}

object ComputeCooccurrenceMatrixPairs extends Configured with Tool with WritableConversions with Tokenizer {
  val log = Logger.getLogger(getClass.getName)

  class MyMapper extends Mapper[LongWritable, Text, PairOfStrings, IntWritable] {
    var window = 2

    override def setup(context: Mapper[LongWritable, Text, PairOfStrings, IntWritable]#Context) {
      window = context.getConfiguration.getInt("window", 2)
    }

    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, PairOfStrings, IntWritable]#Context) = {
      val tokens = tokenize(value)
      for (i <- tokens.indices) {
        for (j <- Math.max(i - window, 0) until Math.min(i + window + 1, tokens.length)) {
          if (i != j) context.write(new PairOfStrings(tokens(i), tokens(j)), 1)
        }
      }
    }
  }
 
 class MyCombiner extends Reducer[PairOfStrings, IntWritable, PairOfStrings, IntWritable] {
    var SUM: IntWritable = new IntWritable(1);

    @Override
    override def reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      var sum: int = 0
      while (iter.hasNext()) {
        sum += iter.next().get()
      }
      SUM.set(sum)
      context.write(key, SUM)
    }
}

 
  class MyReducer extends Reducer[PairOfStrings, IntWritable, PairOfStrings, PairOfIntFloat] {
   var PMI: PairOfIntFloat = new PairOfIntFloat(1,1);
    override def reduce(key: PairOfStrings, values: java.lang.Iterable[IntWritable],
                        context: Reducer[PairOfStrings, IntWritable, PairOfStrings, PairOfIntFloat]#Context) = {
      var sum = 0
      for (value <- values.asScala) {
        sum += value
      }
     PMI.set(sum,1)
      context.write(key,  PMI)
    }
  }

  class MyPartitioner extends Partitioner[PairOfStrings, IntWritable] {
    override def getPartition(key: PairOfStrings, value: IntWritable, numReduceTasks: Int): Int = {
      return (key.getLeftElement.hashCode & Integer.MAX_VALUE) % numReduceTasks
    }
  }

  override def run(argv: Array[String]) : Int = {
    val args = new ConfPairs(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Window: " + args.window())

    val conf = getConf()
    val job = Job.getInstance(conf)

    FileInputFormat.addInputPath(job, new Path(args.input()))
    FileOutputFormat.setOutputPath(job, new Path(args.output()))

    job.setJobName("Cooccurrence Pairs")
    job.setJarByClass(this.getClass)

    job.setMapOutputKeyClass(classOf[PairOfStrings])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[PairOfStrings])
    job.setOutputValueClass(classOf[PairOfIntFloat])
    job.setOutputFormatClass(classOf[TextOutputFormat[PairOfStrings, PairOfIntFloat]])

    job.setMapperClass(classOf[MyMapper])
    job.setCombinerClass(classOf[MyCombiner])
    job.setReducerClass(classOf[MyReducer])
    job.setPartitionerClass(classOf[MyPartitioner])

    job.getConfiguration.setInt("window", args.window())

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
