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
    val custMap:HashMap[Int,String] = HashMap()
    val nationMap:HashMap[Int,String] = HashMap()

    val cust = sc.textFile(args.input() + "/customer.tbl")
    cust
      .map(line => {
        val a = line.split("\\|")
        (a(0), a(3))
      })
      .collect()
      .foreach(p => {
        partMap += (p._1.toInt -> p._3)
      })

    val nation = sc.textFile(args.input() + "/nation.tbl")
    nation
      .map(line => {
        val a = line.split("\\|")
        (a(0), a(1))
      })
      .collect()
      .foreach(p => {
        nationMap += (p._1.toInt -> p._2)
      })

    val bCustMap = sc.broadcast(custMap)
    val bNationMap = sc.broadcast(nationMap)
     
    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
    .filter(line => {
        line.split("\\|")(10) contains date
      })
    .map(line => {
      tokens = line.split('|')
      (tokens(0), tokens(10))
    })
     
    val orders = sc.textFile(args.input() + "/orders.tbl")
    .map(line => {
      tokens = line.split('|')
      (tokens(0), tokens(1))
    })
    .cogroup(lineitems)
    .filter(_._2._1.size != 0)
    .map(line => {
      val temp = bCusMap.value(line._2._1.iterator.next())
      (temp,1)
    })
    .reduceByKey(_,_)
    .sortByKey()
    .collect()
    .foreach(line => {
      println((line._1, bNationMap.value(p._1), p._2))
    })
   }
}
     
    
