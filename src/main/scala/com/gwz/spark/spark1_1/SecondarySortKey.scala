package com.gwz.spark.spark1_1

class SecondarySortKey(val first:Double,val second:Double)
  extends Ordered[SecondarySortKey] with Serializable {
  override def compare(other: SecondarySortKey): Int = {
    if(this.first - other.first != 0){
      (this.first - other.first).toInt
    }else{
      if(this.second - other.second > 0){
        //求不小于给定实数的最小整数
        Math.ceil(this.second - other.second).toInt
      }else if(this.second - other.second < 0){
        //向下取整
        Math.floor(this.second - other.second).toInt
      }else{
        (this.second - other.second).toInt
      }
    }
  }
}
