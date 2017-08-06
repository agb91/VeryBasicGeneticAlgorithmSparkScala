package com.pack.simpler

class Animal extends Serializable{
  
  var speed : Double = 0.0
  var dimension : Double = 0.0
  var lowWaterNeed : Double = 0.0
  var lowFoodNeed : Double = 0.0
  
  
  override def toString: String =
    s"($speed, $dimension, $lowWaterNeed, $lowFoodNeed)"
  
  def isPossible() : Boolean =
  {
    if( dimension + speed > 6 )
    {
      return false
    }
    if( dimension + lowWaterNeed > 6 )
    {
      return false
    }
    if( dimension + lowFoodNeed > 6 )
    {
      return false
    }
    if( (dimension + speed + lowFoodNeed + lowWaterNeed) > 15 )
    {
      return false
    }
    if(dimension < 0)
    {
      return false
    }
    if(speed < 0 )
    {
      return false
    }
    if(lowFoodNeed < 0 )
    {
      return false
    }
    if(lowWaterNeed < 0 )
    {
      return false
    }
    return true
  }
  
}