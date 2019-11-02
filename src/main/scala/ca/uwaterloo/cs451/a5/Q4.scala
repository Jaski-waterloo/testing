package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.util.Try

class ConfQ3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
//   val output = opt[String](descr = "output path", required = true)
//   val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
//   val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
  val date = opt[String](descr = "date of Select Query", required = true)
  val text = opt[Boolean](descr = "Use Text Data", required = false)
  val parquet = opt[Boolean](descr = "Use parquet Data", required = false)
  verify()
}

object Q3 extends Tokenizer 
{
   val log = Logger.getLogger(getClass().getName())
  
//   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  
   def main(argv: Array[String]) {
    val args = new ConfQ3(argv)

    log.info("Input: " + args.input())
    log.info("Date : " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
     log.info("Text Data : " + args.text())
     log.info("Parquet Data : " + args.parquet())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)
     
//      val count = sc.accumulator(0, "accumulator");
//      val date = sc.broadcast(args.date())
    val date = args.date()
    val partMap:HashMap[Int,String] = HashMap()
    val suppMap:HashMap[Int,String] = HashMap()

    val part = sc.textFile(args.input() + "/part.tbl")
    part
      .map(line => {
        val a = line.split("\\|")
        (a(0), a(1))
      })
      .collect()
      .foreach(p => {
        partMap += (p._1.toInt -> p._2)
      })

    val supplier = sc.textFile(args.input() + "/supplier.tbl")
    supplier
      .map(line => {
        val a = line.split("\\|")
        (a(0), a(1))
      })
      .collect()
      .foreach(p => {
        suppMap += (p._1.toInt -> p._2)
      })

    val bPartMap = sc.broadcast(partMap)
    val bSuppMap = sc.broadcast(suppMap)
