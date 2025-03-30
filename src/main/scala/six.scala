object six {

  import org.apache.spark.sql.functions.{col, column, when}
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.sql.{SaveMode, SparkSession}
  import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}



  //
  //import org.apache.spark.SparkContext


    def main(args: Array[String]): Unit = {


      //
      //    val spark=SparkSession.builder()
      //      .appName("spark-program")
      //      .master("local[*]")
      //      .getOrCreate()

      //    val scchema="id Int,name String,salary Int, city String"

      val schema=StructType(List(
        StructField("id",IntegerType),
        StructField("name",StringType),
        StructField("salary",IntegerType),
        StructField("city",StringType)
      ))

      val conf=new SparkConf()
      conf.set("spark.app.name","spark-program")
      conf.set("spark.master","local[*]")



      val spark=SparkSession.builder()
        .config(conf)
        .getOrCreate()



      //    val  df=spark.read
      //      .format("csv")
      //      .option("header","true")
      //      .schema(schema)
      //      .option("path","C:/Users/Karthik Kondpak/Documents/details.csv")
      //      .load()


      //    val data=List((1,"manoj",56),(2,"vinay",78),(3,"mohan",67),(4,"veer",78))
      //
      //
      //    val df=spark.createDataFrame(data)
      //
      //    val df2=df.toDF("id","name","age")
      //
      //    df2.show()

      import spark.implicits._


//      val data=List((1,"manoj",56),(2,"vinay",78),(3,"mohan",67),(4,"veer",78)).toDF("id","name","age")
//
//
//      val df1=data.withColumn("pension",when(col("age")>55,"eligible").otherwise("noteligible"))
//        .withColumn("details",when(col("id")>3,"senior").otherwise("junior"))
//      df1.show()


//      You have a DataFrame employees with columns: employee_id, name, age, and salary.
//       Create a new column age_group based on age:
//        o 'Young' if age < 30
//      o 'Mid' if 30 <= age <= 50
//      o 'Senior' if age > 50
//       Create a new column salary_range based on salary:
//        o 'High' if salary > 100000
//      o 'Medium' if 50000 <= salary <= 100000
//      o 'Low' if salary < 50000
//       Filter employees whose name starts with 'J'.
//       Filter employees whose name ends with 'e'

//      val df3 = List((1, "John", 28, 60000), (2, "Jake", 35, 120000), (3, "Alice", 55, 90000), (4, "Steve", 45, 40000), (5, "Jane", 29, 75000)
//      ).toDF("employee_id", "name", "age", "salary")
//
//
//      val df4 = df3.withColumn("age_group", when(col("age") < 30, "Young")
//          .when(col("age") >= 30 && col("age") <= 50, "Mid")
//          .otherwise("Senior")
//      )
//      df4.show()
//
//      // Adding salary_range column
//      val df5 = df4.withColumn("salary_range", when(col("salary") > 100000, "High")
//          .when(col("salary") >= 50000 && col("salary") <= 100000, "Medium")
//          .otherwise("Low")
//      )
//
//      // Filter employees whose name starts with 'J'
//      val df6 = df5.filter(col("name").startsWith("J"))
//
//      // Filter employees whose name ends with 'e'
//      val df7 = df5.filter(col("name").endsWith("e"))
//
//      // Show results
//      println("Employees whose name starts with 'J':")
//      df6.show()
//
//      println("Employees whose name ends with 'e':")
//      df7.show()


  //      [09:54, 30/3/2025] Karthik Seekho Bigdata Institute: Problem:
  //      You have a DataFrame work_hours with columns: employee_id, work_date, hours_worked, and
  //      department.
  //       Create a new column hours_category based on hours_worked:
  //        o 'Overtime' if hours_worked > 8
  //      o 'Regular' if hours_worked <= 8
  //       Filter work hours where department starts with 'S'.
  //      [09:54, 30/3/2025] Karthik Seekho Bigdata Institute: employee_id work_date hours_worked department
  //        1 2024-01-10 9 Sales
  //        2 2024-01-11 7 Support
  //        3 2024-01-12 8 Sales
  //        4 2024-01-13 10 Marketing
  //        5 2024-01-14 5 Sales
  //        6 2024-01-15 6 Support


      val employeeData = List(
        (1, "2024-01-10", 9, "Sales"),
        (2, "2024-01-11", 7, "Support"),
        (3, "2024-01-12", 8, "Sales"),
        (4, "2024-01-13", 10, "Marketing"),
        (5, "2024-01-14", 5, "Sales"),
        (6, "2024-01-15", 6, "Support")
      ).toDF("employee_id", "work_date", "hours_worked", "department")


      val df=employeeData.withColumn("hours_category",when(col("hours_worked")>8,"Overtime").otherwise("Regular"))
      df.show()
      val df1=df.filter(col("department").startsWith("s"))
      df1.show()






}
}