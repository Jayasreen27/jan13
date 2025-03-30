import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._



object five {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.set("spark.app.name","spark-program")
    conf.set("spark.master","local[*]")



    val spark=SparkSession.builder()
      .config(conf)
      .getOrCreate()


//
//    val  df=spark.read
//      .format("csv")
//      .option("header","true")
//      .option("path","/Users/kalky/Documents/file operators folder/info.csv")
//      .load()
//
//
//
//    df.show()


//        val data=List((1,"manoj",56),(2,"vinay",78),(3,"mohan",67),(4,"veer",78))
//
//
//        val df=spark.createDataFrame(data)
//
//        val df2=df.toDF("id","name","age")
//
//        df2.show()

    import spark.implicits._


    val data = List((1, "manoj", 56), (2, "vinay", 78), (3, "mohan", 67), (4, "veer", 78)).toDF("id", "name", "age")


   val df = List(("A", 25), ("B", 30), ("C", 35)).toDF("name", "age")
//    val df2 = df.withColumn("Category",
//      when(col("age") < 30, "Young").
//        otherwise("Old"))

    val df1 = df.select(
      col("name"),
      col("age"),
      when(col("age") < 30, "Young").otherwise("Old").alias("Category") // Alias added
    )

    df1.show()


//    // Register DataFrame as a Temporary View
//    df.createOrReplaceTempView("people")
//
//    // Running Spark SQL Query
//    val resultDF = spark.sql("""
//    SELECT name, age,
//           CASE WHEN age < 30 THEN 'Young'
//                ELSE 'Old'
//           END AS Category
//    FROM people
//""")
//
//    // Show Result
//    resultDF.show()

  }
// pyspark solution
//  data1= [("A", 25), ("B", 30), ("C", 35)]
//  schema1=["name","age"]
//  df=spark.createDataFrame(data1,schema1)
//  df2 = df.withColumn("category", when(df.age < 30, "Young") .when(df.age > 30, "OLD"))
//  df2.show()





}
