package ca.uwaterloo.cs451.a2;

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
    def main(args: Array[String]) {
      val inputFile = args(0)
      val outputFile = args(1)
      val conf = new SparkConf().setAppName("wordCount")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      val counts =  sc.textFile(inputFile).flatMap(line => line.take(40).split(" ")).map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Split up into words.
//       val words = input.flatMap(line => line.take(40).split(" "))
      // Transform into word and count.
//       val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
//       counts.saveAsTextFile(outputFile)
        val rdd=spark.sparkContext.parallelize(counts)
    }
}
