package com.gwz.spark.spark1_1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
/**
  * DataFrame实现电影分析
  */
object FilmAnalysisDataFrame {

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
    //功能一：实现某部电影观看者中男性和女性不同年龄人数
    //UserId::Gender(性别：F/M)::Age::Occupation(职业)::Zip-code(邮编)
    val userRDD = sc.textFile(dataPath+"users.dat")
    //UserId::MovieID::Rating(评分,满分是5分)::Timestamp
    val ratingsRDD = sc.textFile(dataPath+"ratings.dat")
    //MovieID::Title(电影名称)::Genres(电影类型)
    val moviesRDD = sc.textFile(dataPath+"movies.dat")

    val schemaForUser = StructType(
      "UserId::Gender::Age::Occupation::Zip-code".split("::")
        .map(column => StructField(column,StringType,true)))

    val schemaForRatings = StructType("UserId::MovieID".split("::").map(column=>StructField(column,StringType,true)))
      .add("Rating",StringType,true)
      .add("Timestamp",StringType,true)

    val schemaForMovie = StructType(
      "MovieID::Title::Genres".split("::")
        .map(column => StructField(column,StringType,true)))

    val userRDDRows = userRDD.map(_.split("::"))
      .map(line=>Row(line(0).trim,line(1).trim,line(2).trim,line(3).trim,line(4).trim))

    val ratingsRDDRows = ratingsRDD.map(_.split("::")).map(line=>Row(line(0).trim,line(1).trim,line(2).trim,line(3).trim))

    val moviesRDDRows = moviesRDD.map(_.split("::")).map(line=>Row(line(0).trim,line(1).trim,line(2).trim))


    val userDataFrame = spark.createDataFrame(userRDDRows,schemaForUser)
    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows,schemaForRatings)
    val moviesDataFrame = spark.createDataFrame(moviesRDDRows,schemaForMovie)

    ratingsDataFrame.filter(s"MovieID = 1193")
      .join(userDataFrame,"UserID")
      .select("Gender","Age")
      .groupBy("Gender","Age")
      .count().show(10)

    /*******************************************************************************************************************/

    //功能二：用LocalTempView实现某部电影观看者中不同性别不同年龄分别有多少人

    //第一种方式:创建会话级别的临时表
    //这种创建临时表的方式是会话级别的
    ratingsDataFrame.createTempView("ratings")
    userDataFrame.createTempView("users")

    val sql_local = "select " +
                        "Gender," +
                        "Age," +
                        "count(*) " +
                    "from " +
                        "users u join ratings as r " +
                    "on " +
                        "u.UserID = r.UserID " +
                    "where " +
                        "MovieID = 1193 " +
                    "group by " +
                        "Gender,Age"
    spark.sql(sql_local).show(10)

    //方式二：创建Application级别的临时表，但是这样就要在写SQL语句时在表名前面加上 globa_temp
    ratingsDataFrame.createGlobalTempView("ratings")
    userDataFrame.createGlobalTempView("users")
    val sql = "select Gender,Age,count(*) from globa_temp.users u join globa_temp.ratings as r on u.UserID = r.UserID where MovieID = 1193 group by Gender,Age"

    //可以引入一个隐身转换来实现一个复杂的功能
    import spark.sqlContext.implicits._
    ratingsDataFrame.select("MovieID","Rating").groupBy("MovieID").avg("Rating")
    //接着可以使用 '$' 符号把引号里的字符串转换成列来实现复杂的功能
    //例如，把 avg(Rating)作为排序的字段降序排列
      .orderBy($"avg(Rating)".desc).show(10)

    /******************************************************************************************************************/
    ratingsDataFrame.select("MovieID","Rating").groupBy("MovieID").avg("Rating")
    //这里直接使用DataFrame的RDD方法转换到RDD的操作
      .rdd.map(row=>(row(0),row(1)))
      .sortBy(_._1.toString.toDouble,false)
      .map(tupple=>tupple._2)
      .collect().take(10).foreach(println)


    sc.stop()

  }
}
