package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.util.Try

class ConfQ4(args: Seq[String]) extends ScallopConf(args) {
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

object Q4 extends Tokenizer 
{
   val log = Logger.getLogger(getClass().getName())
  
//   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  
   def main(argv: Array[String]) {
    val args = new ConfQ4(argv)

    log.info("Input: " + args.input())
    log.info("Date : " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
     log.info("Text Data : " + args.text())
     log.info("Parquet Data : " + args.parquet())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)
     
//      val count = sc.accumulator(0, "accumulator");
//      val date = sc.broadcast(args.date())
    val date = args.date()

    val customer = sc.textFile(args.input() + "/customer.tbl")
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, a(3).toInt)
      })
      .collectAsMap()

    val nation = sc.textFile(args.input() + "/nation.tbl")
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, a(1).toInt)
      })
      .collectAsMap()
    

    val bCusMap = sc.broadcast(customer)
    val bNatMap = sc.broadcast(nation)

    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
      .filter(line => {
        line.split("\\|")(10) contains date
      })
      .map(line => {
        (line.split("\\|")(0).toInt, 0)
      })

    val orders = sc.textFile(args.input() + "/orders.tbl")
    orders
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, a(1).toInt)
      })
      .cogroup(lineitems)
      .filter(p => {
        !p._2._2.isEmpty
      })
      .map(p => {
        val nkey = bCusMap.value(p._2._1.iterator.next())
        (nkey, 1) 
      })
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()
      .foreach(p => {
        println((p._1, bNatMap.value(p._1), p._2))
      })

  }
}





     
    
