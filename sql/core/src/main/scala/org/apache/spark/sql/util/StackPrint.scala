package org.apache.spark.sql.util

/**
  * Created by admin on 18/4/8.
  */
object StackPrint {

  def stackPrint() : Unit = {
    try{
      throw  new   RuntimeException()
    }catch {
      case e : Exception =>
        e.printStackTrace()
    }
  }
}
