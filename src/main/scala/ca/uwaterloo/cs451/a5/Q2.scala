package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ConfQ2(args: Seq[String]) extends ScallopConf(args) {
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

object Q2 extends Tokenizer 
{
   val log = Logger.getLogger(getClass().getName())
  
//   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  
   def main(argv: Array[String]) {
    val args = new ConfQ2(argv)

    log.info("Input: " + args.input())
    log.info("Date : " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
     log.info("Text Data : " + args.text())
     log.info("Parquet Data : " + args.parquet())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)
     
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
    val orders = sc.textFile(args.input() + "/orders.tbl")
     
//      val count = sc.accumulator(0, "accumulator");
//      val date = sc.broadcast(args.date())
     val date = args.date();

    orders.map(line=> {
      val tokens = line.split('|')
      (tokens(0),tokens(6))
    })
     .collectAsMap()
     
    val ordersBroadcast = sc.broadcast(orders)
    val counts = 0
    var queryOutput = scala.collection.mutable.ListBuffer[(String, String)]()
     
     
     lineitem.map(line => {
       val tokens = line.split('|')
       (tokens(0),tokens(10))
     })
     .filter((pair) => pair._2 contains date)
     .foreach(line => {
       if(counts < 20){
         if(ordersBroadcast.value(line._1)){
           var output : (String, String) = (ordersBroadcast.value(line._1), line._1)
           queryOutput += output
         }
       }
     })
     
     println("ANSWER=");
     for(output <- queryOutput){
       println("(" + output._1 + "," + output._2 + ")")
     }
   }
}
