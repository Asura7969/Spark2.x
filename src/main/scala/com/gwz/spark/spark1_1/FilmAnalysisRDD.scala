package com.gwz.spark.spark1_1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * RDD实现电影分析
  */
object FilmAnalysisRDD {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDD_Movie_Users_Analyzer")
      .setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //获取SparkSession的SparkContext对象
    val sc = spark.sparkContext
    //把Spark程序的运行日志设置成warn级别
    sc.setLogLevel("warn")

    val dataPath = ""
    //UserId::Gender(性别：F/M)::Age::Occupation(职业)::Zip-code(邮编)
    val userRDD = sc.textFile(dataPath+"users.dat")
    //MovieID::Title(电影名称)::Genres(电影类型)
    val moviesRDD = sc.textFile(dataPath+"movies.dat")
    //UserId::MovieID::Rating(评分,满分是5分)::Timestamp
    val ratingsRDD = sc.textFile(dataPath+"ratings.dat")

    //所有电影中平均得分最高的电影
    val moveInfo = moviesRDD.map(_.split("::")).map(x=>(x(0),x(1))).cache()
    val ratings = ratingsRDD.map(_.split("::")).map(x=>(x(0),x(1),x(2))).cache()

    val movesAndRatings = ratings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val avgRatings = movesAndRatings.map(x=>(x._1,x._2._1.toDouble/x._2._2))
    avgRatings.join(moveInfo).map(item=>(item._2._1,item._2._2))
      .sortByKey(false).take(10)
      .foreach(record=>println(record._2 + "评分为:" + record._1))

    //最受男性和女性喜爱的top10电影
    val userGender = userRDD.map(_.split("::")).map(x=>(x(0),x(1)))
    val genderRatings = ratings.map(x=>(x._1,(x._1,x._2,x._3))).join(userGender).cache()
    genderRatings.take(10).foreach(println)

    val maleFilteredRatings = genderRatings.filter(x=>x._2._2.equals("M")).map(x=>x._2._1)
    val femaleFilteredRatings = genderRatings.filter(x=>x._2._2.equals("F")).map(x=>x._2._1)

    //对电影评分数据以Timestamp和Rating两个维度进行二次降序排序
    val pairWithSortKey = ratingsRDD.map(line=>{
      val split = line.split("::")
      (new SecondarySortKey(split(3).toDouble,split(2).toDouble),line)
    })
    val sorted = pairWithSortKey.sortByKey(false)
    sorted.map(sortedline => sortedline._2).take(10).foreach(println)

    sc.stop()
  }
}
