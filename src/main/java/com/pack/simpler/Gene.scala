package com.pack.simpler

class Gene extends Serializable{
  
  var number : Double = 0.0
  
  override def toString: String =
    s"($number)"
  
}