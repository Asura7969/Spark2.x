# Spark2.x

**本项目以spark2.2.0为基础版本**

__暂时只涉及到以下简单API操作:__   
* [RDD](https://github.com/Asura7969/Spark2.x/blob/master/src/main/scala/com/gwz/spark/spark1_1/FilmAnalysisRDD.scala)  
* [DataFrame](https://github.com/Asura7969/Spark2.x/blob/master/src/main/scala/com/gwz/spark/spark1_1/FilmAnalysisDataFrame.scala)  
* [DataSet](https://github.com/Asura7969/Spark2.x/blob/master/src/main/scala/com/gwz/spark/spark1_1/FilmAnalysisDataSet.scala)  
* [Structured Streaming](https://github.com/Asura7969/Spark2.x/blob/master/src/main/scala/com/gwz/spark/spark2_3/StreamStructured.scala)  
* [累加器](https://github.com/Asura7969/Spark2.x/blob/master/src/main/scala/com/gwz/spark/spark2_2/MyAccumulator.scala)  
* [二次排序](https://github.com/Asura7969/Spark2.x/blob/master/src/main/scala/com/gwz/spark/spark1_1/SecondarySortKey.scala)

## RDD弹性特性七个方面的解析:
1. 自动进行内存和磁盘数据存储
    >计算时优先内存
2. 基于Lineage(血统)的高效容错机制
    >Lineage是基于RDD的依赖关系完成的(宽依赖和窄依赖)  
    常规的容错方式: 数据检查点; 记录数据更新
3. Task失败,会自动进行特定次数的重试
    >默认重试次数4次  
    源码:TaskSchedulerImpl.scala
4. Stage如果失败,会自动进行特定次数的重试
    >默认重试次数4次  
    源码:Stage.scala
5. checkpoint和persist(检查点和持久化),可主动或被动触发
6. 数据调度弹性,DAGScheduler、TaskScheduler和资源管理无关
7. 数据分片的盖度弹性(coalesce)

## DataSet  
>DataSet是“懒加载”的,即只有在action算子被触发时才进行计算  
#####DataSet创建方式:  
    
    val people = spark.read.parquet("...").as[Person]  //scala
    Dataset<Person> people = spark.read.parquet("...").as(Encoders.bean(Person.class))  //java  

#####Example:
***___scala版___***

    //使用sparkSession创建DataSet[Row]
    val people = spark.read.parquet("...")
    val department = spark.read.parquet("...")
    
    people.filter("age > 30")
        .join(department,people("deptId") === department("id"))
        .groupBy(department("name"),"gender")
        .agg(avg(people("salary")),max(people("age"))) 
        
***___java版___***  

    //To create DataSet<Row> using SparkSession
    DataSet<Row> people = spark.read().parquet("...")
    DataSet<Row> department = spark.read().parquet("...")
    
    people.filter("age".gt(30))
        .join(department,people.col("deptId").equalTo(department("id")))
        .groupBy(department.col("name"),"gender")
        .agg(avg(people.col("salary")),max(people.col("age")))




*后续会持续更新...*
