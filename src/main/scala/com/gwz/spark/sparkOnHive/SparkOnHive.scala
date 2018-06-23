package com.gwz.spark.sparkOnHive

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkOnHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkOnHive").setMaster("local")
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir","/User/gongwenzhou/spark-warehouse")
      .enableHiveSupport().getOrCreate()

    spark.sql("CREATE TABLE IF NOT EXISTS src(key INT,value STRING)")
    spark.sql("LOAD DATA LOCAL INPATH 'data/peoplemanagedata/kv1.txt' INTO TABLE src")
    spark.sql("SELECT * FROM src").show()
    spark.sql("SELECT COUNT(*) FROM src").show()


    case class People(name:String,age:Long)
    case class PersonScore(n:String,score:Long)
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //读取json文件  DataFrame  与  Dataset  相互转换
    val people: DataFrame = spark.read.json("data/peoplemanagedata/people.json")
    val peopleDs: Dataset[People] = people.as[People]

    peopleDs.printSchema()
    val peopleToDF = peopleDs.toDF()

    //map
    val personDS: Dataset[(String, Long)] = peopleDs.map(person => {
      (person.name, person.age)
    })
    personDS.show()

    //flatMap   模式匹配:如果姓名是Andy则年龄加上70,其余员工加上30
    peopleDs.flatMap(person=>person match{
      case People(name,age) if name.equals("Andy") => List(name,age + 70)
      case People(name,age) => List(name,age + 30)
    }).show()


    /**
      * dropDuplicates 算子  删除重复元素
      * 与 distinct 比较：distinct是从Dataset中返回一个新的数据集，新的数据集包含唯一的记录行
      * distinct  是  dropDuplicates的别名
      */
    peopleDs.dropDuplicates("name").show()

    //sort算子 对年龄进行降序排序
    peopleDs.sort($"age".desc).show()

    //join算子
    val personScoreDs = spark.read.json("data/peoplemanagedata/peopleScore.json").as[PersonScore]
    peopleDs.join(personScoreDs,$"name" === $"n").show()

    //joinWith 对数据集进行内关联,当关联的姓名相等时,返回一个tuple键值对,格式为(年龄,姓名),(姓名,评分)
    peopleDs.joinWith(personScoreDs,$"name" === $"n").show()
    peopleDs.joinWith(personScoreDs,peopleDs.col("name") === personScoreDs.col("n")).show()

    /**
      * randomSplit  对peopleDs进行随机切分
      * 参数：表示权重,权重越大,dataset中被分到的people个数就越多
      */
    peopleDs.randomSplit(Array(20,10)).foreach(dataset=>{
      dataset.show()
    })

    //内置函数 cancat
    peopleDs.groupBy($"name",$"age").agg(concat($"name",$"age")).show()

    /**
      * 内置函数 collect_list collect_set
      * collect_list  无重复
      * collect_set   有重复
      */
    peopleDs.groupBy($"name").agg(collect_list("name"),collect_set("name"))



    spark.stop()

  }
}
