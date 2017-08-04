package com.pack.simpler

import org.apache.spark.SparkConf
import scala.util.Random
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


class Work{
  
  val map: ( RDD[ ( String , Gene ) ] , Double ) => RDD[ (String , Gene) ] =
   ( input: RDD[ (String , Gene) ] , best:Double) =>
  {
    
    var old : Gene = new Gene()
    
    val result = input.map( gene =>
      {
        var toAdd : Gene = new Gene() with Serializable
        toAdd.number = ((gene._2.number + old.number) * 0.65) + (best * 0.5 ) 
        ( "creature" , toAdd )
      })
    //println( result.collect().mkString("new generation: ", "-", "|") )  
    result  
      
  }
  
  val reduce: ( RDD[ (String , Gene) ] ) => RDD[ (String , Gene) ] =
     (mappedRDD : RDD[ (String , Gene) ] ) => {
    var result = mappedRDD.reduceByKey( accumulate )  
    result
  }
  
   def accumulate (accumulator: Gene, toAdd: Gene )
  : Gene =
  {
    if( accumulator.number > toAdd.number )
    {
      accumulator
    }
    else
    {
      toAdd
    }
  }
  

  
}