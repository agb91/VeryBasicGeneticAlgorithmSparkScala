package com.pack.simpler

import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.simpler.Gene


object MainAnalyzer {
  
  def firstGenerator: ( Int , SparkContext ) => RDD[ ( String , Gene ) ] =
     ( n : Int , sc : SparkContext ) =>
 {
   var arrayResult = new Array[ (String , Gene) ]( n )
   
   var a = 0;
   val r = scala.util.Random   
    for( a <- 0 to (n-1) ){
      var g : Gene = new Gene() with Serializable
      
      g.number = r.nextInt(10)
      arrayResult( a ) = ( "creature" , g )
    }
    
    sc.parallelize( arrayResult , 4 )
    
 }
  
  def main(args: Array[String]) = {
    
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("MainAnalyzer")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
    var work : Work = new Work() with Serializable
    
    var generation = firstGenerator( 10 , sc )
   
    var a = 0;
    var bestSeed = 0.0
    for( a <- 0 to (25) )
    {
      
      generation = work.map(generation , bestSeed)
      var best = work.reduce(generation)
      bestSeed = best.collect()(0)._2.number
      println( "name: " + best.collect()(0)._1 + ";  number: " + best.collect()(0)._2.number )
    }
        
     sc.stop 
  }
}