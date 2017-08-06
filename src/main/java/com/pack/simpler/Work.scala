package com.pack.simpler

import org.apache.spark.SparkConf
import scala.util.Random
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


class Work{
  
  val map: ( RDD[ ( String , Animal ) ] , Animal ) => RDD[ (String , Animal) ] =
   ( input: RDD[ (String , Animal) ] , best:Animal) =>
  {
    
    val result = input.map( animal =>
      {
        var continue = true
        var toAdd : Animal = new Animal() with Serializable
        do{
          var r : Random = new Random() with Serializable
          toAdd.speed = (animal._2.speed * 0.2 + best.speed * 0.8) + r.nextInt(2) - r.nextInt(2)
          toAdd.dimension = (animal._2.dimension * 0.2 + best.dimension * 0.8) + r.nextInt(2) - r.nextInt(2)
          toAdd.lowFoodNeed = (animal._2.lowFoodNeed * 0.2 + best.lowFoodNeed * 0.8) + r.nextInt(2) - r.nextInt(2)
          toAdd.lowWaterNeed = (animal._2.lowWaterNeed * 0.2 + best.lowWaterNeed * 0.8) + r.nextInt(2) - r.nextInt(2)
          if( toAdd.isPossible() == true )
          {
            continue = false
          }
        } while (continue)
        ( "creature" , toAdd )
      })
    //println( result.collect().mkString("new generation: ", "-", "|") )  
    result  
      
  }
  
  val reduce: ( RDD[ (String , Animal) ] ) => RDD[ (String , Animal) ] =
     (mappedRDD : RDD[ (String , Animal) ] ) => {
    var result = mappedRDD.reduceByKey( accumulate )  
    result
  }
  
   def accumulate (accumulator: Animal, toAdd: Animal )
  : Animal =
  {
     
    if( points(accumulator) > points(toAdd) )
    {
      accumulator
    }
    else
    {
      toAdd
    }
  }
   
   //case desert
   def points( animal : Animal ) : Double =
   {
     var result : Double = 0.0
     result = result + animal.lowWaterNeed * 100
     result = result + animal.lowFoodNeed * 10
     result = result + animal.dimension
     result = result + animal.speed * 2
     return result
   }
  

  
}