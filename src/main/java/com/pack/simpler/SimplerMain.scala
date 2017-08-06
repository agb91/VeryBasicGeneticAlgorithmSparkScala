package com.pack.simpler

import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.simpler.Animal


object MainAnalyzer {
  
  def firstGenerator: ( Int , SparkContext ) => RDD[ ( String , Animal ) ] =
     ( n : Int , sc : SparkContext ) =>
 {
   var arrayResult = new Array[ (String , Animal) ]( n )
   
   var a = 0;
   val r = scala.util.Random   
   var continue = true
    for( a <- 0 to (n-1) ){
      do{
        var g : Animal = new Animal() with Serializable
        g.speed = r.nextInt(5)
        g.dimension = r.nextInt(3)
        g.lowFoodNeed = r.nextInt(5)
        g.lowWaterNeed = r.nextInt(5)
        continue = !g.isPossible()
        if(g.isPossible())
        {
          arrayResult( a ) = ( "creature" , g )
        }
      } while ( continue )
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
    var bestExemplary = new Animal()
    for( a <- 0 to (25) )
    {
      
      generation = work.map(generation , bestExemplary)
      var best = work.reduce(generation)
      bestExemplary = best.collect()(0)._2
      println( "name: " + best.collect()(0)._1 + ";  dimension: " + best.collect()(0)._2.dimension
      + ";  speed: " + best.collect()(0)._2.speed    
      + ";  lowWaterNeed: " + best.collect()(0)._2.lowWaterNeed
      + ";  lowFoodNeed: " + best.collect()(0)._2.lowFoodNeed
      )
    }
        
     sc.stop 
  }
}