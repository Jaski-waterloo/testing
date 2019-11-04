package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.util.Try

class ConfQ5(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
//   val output = opt[String](descr = "output path", required = true)
//   val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
//   val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
//   val date = opt[String](descr = "date of Select Query", required = true)
  val text = opt[Boolean](descr = "Use Text Data", required = false)
  val parquet = opt[Boolean](descr = "Use parquet Data", required = false)
  verify()
}

object Q5 extends Tokenizer 
{
   val log = Logger.getLogger(getClass().getName())
  
//   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  
   def main(argv: Array[String]) {
    val args = new ConfQ5(argv)

    log.info("Input: " + args.input())
//     log.info("Date : " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
     log.info("Text Data : " + args.text())
     log.info("Parquet Data : " + args.parquet())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)
     
//      val count = sc.accumulator(0, "accumulator");
//      val date = sc.broadcast(args.date())
//     val date = args.date()

    val customer = sc.textFile(args.input() + "/customer.tbl")
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, a(3).toInt)
      })
     .map(pair => {
       if(pair(1) == 3){
         (pair(0), "CANADA")
       }
       else if(pair(1) == 24)
       {
         (pair(0), "US")
       else
       {
         (pair(0), "NA")
       }
     })
     
    val nation = sc.textFile(args.input() + "/nation.tbl")
     .map(line => {
       val a = line.split('|')
       (a(0).toInt, a(1))
     })
   
    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
     .map(line => {
       val a = line.split('|')
       (a(0).toInt, a(10).substring(0,7))
     })
    
    val bcustomer = sc.broadcast(customer.collectAsMap())
    val bnation = sc.broadcast(nation.collectAsMap())
    
     
    val orders = sc.textFile(args.input() + "/orders.tbl")
     .map(line => {
       val a = line.split('|')
       (a(0).toInt, a(1).toInt)
     })
     .cogroup(lineitems)
     .filter(p => {
       !p._2._2.isEmpty && !p._2._1.isEmpty
     })       
     .filter(p => {
       temp = bnation.value(p._2._1.iterator.next())
       if(temp == "US" || temp =="CANADA") true
       else false
     })
    .flatmap(p => {
      temp = bnation.value(p._2._1.iterator.next())
      while(p._2._2.iterator.hasNext())
      {
        ((temp, p._2._2.iterator.next()), 1)
      }
    .reduceByKey(_+_)
    .collect()
    .foreach(p => {
        println((p._1._1, p._1._2, p._2))
    })
  }
} 
       
     
