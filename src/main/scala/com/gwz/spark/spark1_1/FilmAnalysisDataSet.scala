package com.gwz.spark.spark1_1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * DataSet实现电影分析
  */
object FilmAnalysisDataSet {

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

    import spark.implicits._

    case class User(UserID:String,Gender:String,Age:String,Occupation:String,Zip_Code:String)
    case class Rating(UserID:String,MovieID:String,Rating:Double,Timestamp:String)

    val usersForDSRDD = userRDD.map(_.split("::"))
      .map(line=>User(line(0).trim,line(1).trim,line(2).trim,line(3).trim,line(4).trim))

    val usersDataSet: Dataset[User] = spark.createDataset[User](usersForDSRDD)
    usersDataSet.show(10)

    val ratingsForDSRDD = ratingsRDD.map(_.split("::"))
      .map(line=>Rating(line(0).trim,line(1).trim,line(2).trim.toDouble,line(3).trim))

    val ratingsDataSet: Dataset[Rating] = spark.createDataset[Rating](ratingsForDSRDD)
    ratingsDataSet
      .filter(s"MovieID = 1193")
      .join(usersDataSet,"UserID")
      .select("Gender","Age")
      .groupBy("Gender","Age")
      .count()
      .orderBy($"Gender".desc,$"Age").show()

    //不建议把DataFrame 和 DataSet 混着用，虽然可以但是代码太混乱
    sc.stop()

  }

}
