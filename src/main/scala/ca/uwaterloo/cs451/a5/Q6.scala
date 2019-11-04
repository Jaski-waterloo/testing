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

object Q6 extends Tokenizer 
{
   val log = Logger.getLogger(getClass().getName())
  
//   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  
   def main(argv: Array[String]) {
    val args = new ConfQ6(argv)

    log.info("Input: " + args.input())
    log.info("Date : " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
     log.info("Text Data : " + args.text())
     log.info("Parquet Data : " + args.parquet())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)
     
//      val count = sc.accumulator(0, "accumulator");
//      val date = sc.broadcast(args.date())
    val date = args.date()
    
    val lineitems = sc.textFile(args.input() + "/lineitems.tbl")
    lineitems.
    filter(line => {
      line.split('|')(10) contains date
    })
    .map(line => {
      a = line.split('|')
      retFlag = a(8)
      lineStatus = a(9)
      l_quantity = a(4).toDouble
      l_extendedprice = a(5).toDouble
      l_discount = a(6).toDouble
      l_tax = a(7).toDouble
      
      ((retFlag, lineStatus), (l_quantity, l_extendedprice, l_extendedprice*(1-l_discount), l_extendedprice*(1-l_discount)*(1+l_tax), l_discount, 1))
      
    })
    .reduceByKey((a,b) => {
      (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + a._6)
    })
    .collect()
    .foreach(p => {
      count = p._6
//       l_returnflag,
//       l_linestatus,
      sum_qty = a._1
      sum_base_price = a._2
      sum_disc_price = a._3
      sum_charge = a._4
      avg_qty = a._1 / count
      avg_price = a._2 / count
      avg_disc = a._5 / count
//       count(*) as count_order
      println((sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count))
    })
   }
}
      
      
