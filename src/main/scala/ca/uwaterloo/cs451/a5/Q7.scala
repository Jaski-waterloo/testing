package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.util.Try

class ConfQ7(args: Seq[String]) extends ScallopConf(args) {
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


object Q7 extends Tokenizer
{
  val log = Logger.getLogger(getClass().getName())
  
  //   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  def main(argv: Array[String]) {
    val args = new ConfQ7(argv)
    
    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
    log.info("Text Data? : " + args.text())
    log.info("Parquet Data? : " + args.parquet())
    
    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)
    
    val date = args.date().split('-').map(_.toInt)
    
    val customer = sc.textFile(args.input() + "/customer.tbl")
    .map(line => {
      val a = line.split('|')
      (a(0).toInt, a(1))
    })
    
    val bcustomer = sc.broadcast(customer.collectAsMap())
    
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
    .map(line => {
      val a = line.split('|')
      (a(0).toInt, a(5).toDouble, a(6).toDouble, a(10))
    })
    .filter(p => {
      val date1 = p._4.split('-').map(_.toInt) // year-month-day // lshipdate > date
      (date1(0) > date(0)) || (date1(0) == date(0) && date1(1) > date(1)) || (date1(0) == date(0) && date1(1) == date(1) && date1(2) > date(2))
    })
    
    
    val orders = sc.textFile(args.input + "/orders.tbl")
    
    orders.
    map(line => {
      val a = line.split('|')
      (a(0).toInt, a(1).toInt, a(4), a(7))
    })
    .filter( p => {
      val date1 = p._3.split('-').map(_.toInt) // orderdate < date
      (date1(0) < date(0)) || (date1(0) == date(0) && date1(1) < date(1)) || (date1(0) == date(0) && date1(1) == date(1) && date1(2) < date(2))
    })
    .cogroup(lineitem)
    .saveAsTextFile("myOutput.txt")
  }
}
