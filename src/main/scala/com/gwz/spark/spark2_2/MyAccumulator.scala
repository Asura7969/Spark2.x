package com.gwz.spark.spark2_2

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

/**
  * spark2.x自定义累加器
  */
class MyAccumulator extends AccumulatorV2[String,ArrayBuffer[String]]{

  //设置累加器的结果，类型为 ArrayBuffer[String]
  private var result = ArrayBuffer[String]()

  /** 判断累加器当前的值是否为 "零值",这里我们指定如果result的 size为 0,则累加器当前值是 "零值"
    * @return 返回累加器的值是否为 0
    */
  override def isZero: Boolean = this.result.size == 0

  /** coppy方法设置为新建本累加器,并把result赋给新的累计器
    * 指定对自定义累加器的复制操作
    */
  override def copy(): AccumulatorV2[String, ArrayBuffer[String]] = {
    val newAccum = new MyAccumulator
    newAccum.result = this.result
    newAccum
  }

  /** reset方法把result设置为新的ArrayBuffer
    * 重置累加器
    */
  override def reset(): Unit = this.result == new ArrayBuffer[String]()

  /** add方法把传递进来的字符串添加到result内
    * 指定元素相加操作
    * @param v
    */
  override def add(v: String): Unit = this.result += v

  /** merge方法把两个累加器的result合并起来
    * 合并两个相同类型的累加器
    * @param other
    */
  override def merge(other: AccumulatorV2[String, ArrayBuffer[String]]): Unit = {
    result.++:(other.value)
  }

  /**
    * @return 返回累加器当前的值
    */
  override def value: ArrayBuffer[String] = this.result

  /**
    * 接着在 main 方法里使用累加器
    * val myAMuccm = new MyAccumulator()
    *
    * //向SparkContext注册累加器
    * sc.register(myAMuccm)
    *
    * //把 'a','b','c','d' 添加到累加器中,并打印出来
    * sc.parallelize(Array("a","b","c","d")).foreach(x=>myAMuccm.add(x))
    * println(myAMuccm.value)
    */
}
