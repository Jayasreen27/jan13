import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, _}



object df_assignment {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.set("spark.app.name","spark-program")
    conf.set("spark.master","local[*]")



    val spark=SparkSession.builder()
      .config(conf)
      .getOrCreate()


    import spark.implicits._
    //    1.Conditional Column: Data: A DataFrame employees with columns id, name, age.
    //    val employees = List(
    //      (1, "AJAY", 28),
    //      (2, "VIJAY", 35),
    //      (3, "MANOJ", 22)
    //    ).toDF("id", "name", "age")
    //    Question: How would you add a new column is_adult which is true if the age is greater than or equal
    //    to 18, and false otherwise?

//    val employees = List(
//      (1, "AJAY", 28),
//      (2, "VIJAY", 35),
//      (3, "MANOJ", 22)
//    ).toDF("id", "name", "age")
//
//    val df2 = employees.withColumn("is_adult",
//      when(col("age") >=18, "TRUE").
//        otherwise("FALSE"))
//    df2.show()

//    2)Categorizing Values: Data: A DataFrame grades with columns student_id, score.
//    Question: How would you add a new column grade with values "Pass" if score is greater than or
//    equal to 50, and "Fail" otherwise?


    val grades = List(
      (1, 85),
      (2, 42),
      (3, 73)
    ).toDF("student_id", "score")
//    var df=grades.withColumn("Grade",when(col("score")>=50,"pass").otherwise("Fail"))
//    println("output of with")
//    df.show()

//    var df1=grades.select(col("student_id"),col("score"),when(col("score")>=50,"pass").otherwise("Fail").alias("result"))
//    println("output of select")
//    df1.show()


//    grades.createOrReplaceTempView("data")
//    val df3 = spark.sql("""
//    SELECT student_id, score,
//           CASE WHEN score < 50 THEN 'PASS'
//                ELSE 'Fail'
//           END AS GRADE
//    FROM data
//""")
//    println("output of sparksql")
//
//    df3.show()



//    3)Multiple Conditions: Data: A DataFrame transactions with columns transaction_id, amount.
//    val transactions = List(
//      (1, 1000),
//      (2, 200),
//      (3, 5000)
//    ).toDF("transaction_id", "amount")
//    Question: How would you add a new column category with values "High" if amount is greater than
//      1000, "Medium" if amount is between 500 and 1000, and "Low" otherwise?

//
//    val transactions = List(
//          (1, 1000),
//          (2, 200),
//          (3, 5000)
//        ).toDF("transaction_id", "amount")
//    var df=transactions.withColumn("category",when(col("amount")>1000,"HIGH").
//      when(col("amount").between(500,1000),"medium")
//      .otherwise("LOW"))
//    df.show()
//
//    var df1=transactions.select(col("transaction_id"),col("amount"),when(col("amount")>1000,"HIGH").
//      when(col("amount").between(500,1000),"medium")
//      .otherwise("LOW").alias("category"))
//    df1.show()
//
//    transactions.createOrReplaceTempView("data")
//        val df3 = spark.sql("""
//        SELECT transaction_id, amount,
//               CASE
//                WHEN amount >1000 THEN 'HIGH'
//                WHEN amount between 500 and 1000 THEN 'MEDIUM'
//                    ELSE 'LOW'
//               END AS category
//        FROM data
//    """)
//        println("output of sparksql")
//
//        df3.show()
//
//    4)Conditional Column with String Values: Data: A DataFrame products with columns product_id,
//    price.
//    val products = List(
//      (1, 30.5),
//      (2, 150.75),
//      (3, 75.25)
//    ).toDF("product_id", "price")
//    Question: How would you add a new column price_range with values "Cheap" if price is less than 50,
//    "Moderate" if price is between 50 and 100, and "Expensive" otherwise?



//    val products = List(
//          (1, 30.5),
//          (2, 150.75),
//          (3, 75.25)
//        ).toDF("product_id", "price")
//    var df=products.withColumn("price_range",when(col("price")>50,"Cheap")
//      .when(col("price").between(5,100),"Moderate").otherwise("Expensive"))
//    println("output of with")
//    df.show()
//
//
//        var df1=products.select(col("product_id"),col("price"),when(col("price")>50,"Cheap")
//          .when(col("price").between(5,100),"Moderate").otherwise("Expensive").alias("price_range"))
//        println("output of select")
//        df1.show()
//
//
//        products.createOrReplaceTempView("data")
//        val df3 = spark.sql("""
//        SELECT product_id, price,
//               CASE
//               WHEN price < 50 THEN 'Cheap'
//               WHEN price between 50 and 100 THEN 'Cheap'
//                ELSE 'Expensive'
//               END AS price_range
//        FROM data
//    """)
//        println("output of sparksql")
//
//        df3.show()





//    5)Conditional Column with Date: Data: A DataFrame events with columns event_id, date.
//    val events = List(
//      (1, "2024-07-27"),
//      (2, "2024-12-25"),
//      (3, "2025-01-01")
//    ).toDF("event_id", "date")
//    Question: How would you add a new column is_holiday which is true if the date is "2024-12-25" or
//      "2025-01-01", and false otherwise?
//    val events = List(
//      (1, "2024-07-27"),
//      (2, "2024-12-25"),
//      (3, "2025-01-01")
//    ).toDF("event_id", "date")
//    val df = events.withColumn("is_holiday",
//      when(col("date").isin("2024-12-25", "2025-01-01"), true)
//        .otherwise(false)
//    )
//    df.show()
//
//
//    val df1 = events.select(col("event_id"),col("date"),
//      when(col("date").isin("2024-12-25", "2025-01-01"), true)
//        .otherwise(false).alias("is_holiday"))
//
//    df1.show()
//
//
//    events.createOrReplaceTempView("data")
//    val df3 = spark.sql("""
//        SELECT event_id, date,
//               CASE
//               WHEN date in ("2024-12-25", "2025-01-01") THEN 'TRUE'
//
//                ELSE 'FALSE'
//               END AS is_holiday
//        FROM data
//    """)
//    println("output of sparksql")
//
//    df3.show()






//
//    Medium Questions
//      1)Nested Conditional Column: Data: A DataFrame inventory with columns item_id, quantity.
//    val inventory = List(
//      (1, 5),
//      (2, 15),
//      (3, 25)
//    ).toDF("item_id", "quantity")

//
//        Question: How would you add a new column stock_level with values "Low" if quantity is less than 10,
//        "Medium" if quantity is between 10 and 20, and "High" otherwise?

//        val inventory = List(
//          (1, 5),
//          (2, 15),
//          (3, 25)
//        ).toDF("item_id", "quantity")
//
//        var df=inventory.withColumn("stock_level",when(col("quantity")<10,"Low").
//          when(col("quantity").between(10,20),"medium")
//          .otherwise("HIGH"))
//        df.show()
//
//
//    var df1=inventory.select(col("item_id"),col("quantity"),when(col("quantity")<10,"Low").
//      when(col("quantity").between(10,20),"medium")
//      .otherwise("HIGH").alias("stock_level"))
//    df1.show()
//
//
//    inventory.createOrReplaceTempView("data")
//            val df3 = spark.sql("""
//            SELECT item_id, quantity,
//                   CASE
//                    WHEN quantity <10 THEN 'LOW'
//                    WHEN quantity between 10 and 20 THEN 'MEDIUM'
//                        ELSE 'LOW'
//                   END AS stock_level
//            FROM data
//        """)
//            println("output of sparksql")
//
//            df3.show()




//    Question: How would you add a new column stock_level with values "Low" if quantity is less than 10,
//    "Medium" if quantity is between 10 and 20, and "High" otherwise?
//    Seekho Bigdata Institute
//    2)Conditional String Manipulation: Data: A DataFrame customers with columns customer_id, email.
//    val customers = List(
//      (1, "john@gmail.com"),
//      (2, "jane@yahoo.com"),
//      (3, "doe@hotmail.com")
//    ).toDF("customer_id", "email")
//    Question: How would you add a new column email_provider with values "Gmail" if email contains
//      "gmail", "Yahoo" if email contains "yahoo", and "Other" otherwise?

//
//
//        val customers = List(
//          (1, "john@gmail.com"),
//          (2, "jane@yahoo.com"),
//          (3, "doe@hotmail.com")
//        ).toDF("customer_id", "email")
//
//            var df=customers.withColumn("email_provider",when(col("email")contains("gmail"),"Gmail").
//              when(col("email")contains("yahoo"),"Yahoo")
//              .otherwise("Other"))
//            df.show()
//
//
//    var df1=customers.select(col("customer_id"),col("customer_id"),when(col("email")contains("gmail"),"Gmail").
//      when(col("email")contains("yahoo"),"Yahoo")
//      .otherwise("Other").alias("email_provider"))
//    df1.show()
//
//
//
//    customers.createOrReplaceTempView("data")
//                val df3 = spark.sql("""
//                SELECT customer_id, email,
//                       CASE
//                        WHEN email contains 'google' THEN 'Google'
//                        WHEN email contains 'yahoo' THEN 'Yahoo'
//                            ELSE 'Others'
//                       END AS email_provider
//                FROM data
//            """)
//                println("output of sparksql")
//
//                df3.show()
//




    //    3)Conditional Date Manipulation: Data: A DataFrame orders with columns order_id, order_date.
//    val orders = List(
//      (1, "2024-07-01"),
//      (2, "2024-12-01"),
//      (3, "2024-05-01")
//    ).toDF("order_id", "order_date")
//    Question: How would you add a new column season with values "Summer" if order_date is in June,
//    July, or August, "Winter" if in December, January, or February, and "Other" otherwise?

//
//        val orders = List(
//          (1, "2024-07-01"),
//          (2, "2024-12-01"),
//          (3, "2024-05-01")
//        ).toDF("order_id", "order_date")
//
//    val df = orders.withColumn("season",
//      when(month(col("order_date")) === 6 || month(col("order_date")) === 7 || month(col("order_date")) === 8, "Summer")
//        .when(month(col("order_date")) === 12 || month(col("order_date")) === 1 || month(col("order_date")) === 2, "Winter")
//        .otherwise("Other")
//    )
//    df.show()
//
//
//    orders.createOrReplaceTempView("orders")
//
//    val df2 = spark.sql("""
//    SELECT order_id, order_date,
//           CASE
//               WHEN MONTH(order_date) IN (6, 7, 8) THEN 'Summer'
//               WHEN MONTH(order_date) IN (12, 1, 2) THEN 'Winter'
//               ELSE 'Other'
//           END AS season
//    FROM orders
//""")
//
//    println("Spark SQL:")
//    df2.show()









//    4)Multiple Nested Conditions: Data: A DataFrame sales with columns sale_id, amount.
//    val sales = List(
//      (1, 100),
//      (2, 1500),
//      (3, 300)
//    ).toDF("sale_id", "amount")
//    Question: How would you add a new column discount with values 0 if amount is less than 200, 10 if
//    amount is between 200 and 1000, and 20 if amount is greater than 1000?

//    val sales = List(
//          (1, 100),
//          (2, 1500),
//          (3, 300)
//        ).toDF("sale_id", "amount")
//    var df1= sales.withColumn("discount",when(col("amount")<200 , 0)
//               .when(col("amount").between(200,1000), 10).when(col("amount")>1000, 20))
//    df1.show()
//
//    var df2= sales.select(when(col("amount")<200 , 0)
//      .when(col("amount").between(200,1000), 10).when(col("amount")>1000, 20).alias("discount"))
//    df2.show()
//
//    sales.createOrReplaceTempView("data")
//    val df3 = spark.sql("""
//    SELECT transaction_id, amount,
//           CASE
//               WHEN amount < 200 THEN 0
//               WHEN amount BETWEEN 200 AND 1000 THEN 10
//               WHEN amount > 1000 THEN 20
//               ELSE NULL
//           END AS Discount
//    FROM data
//""")
//    println("output of sparksql")
//
//    df3.show()
//
//
//    sales.createOrReplaceTempView("data")
//
//    // Spark SQL Query
//    val df4 = spark.sql("""
//    SELECT transaction_id, amount,
//           CASE
//               WHEN amount < 200 THEN 0
//               WHEN amount BETWEEN 200 AND 1000 THEN 10
//               WHEN amount > 1000 THEN 20
//               ELSE NULL
//           END AS Discount
//    FROM data
//""")
//
//    println("Output of Spark SQL:")
//
//    df4.show()






//      5)Conditional Column with Boolean Values: Data: A DataFrame logins with columns login_id,
//    login_time.
//      Seekho Bigdata Institute
//    val logins = List(
//      (1, "09:00"),
//      (2, "18:30"),
//      (3, "14:00")
//    ).toDF("login_id", "login_time")
//    Question: How would you add a new column is_morning which is true if login_time is before 12:00,
//    and false otherwise?


//        val logins = List(
//          (1, "09:00"),
//          (2, "18:30"),
//          (3, "14:00")
//        ).toDF("login_id", "login_time")
//        var df1= logins.withColumn("is_morning",when(col("login_time")<"12:00", "TRUE").otherwise("False"))
//
//        df1.show()
//
//        var df2=  logins.select(col("login_id"),col("login_time"),when(col("login_time")<"12:00", "TRUE").otherwise("False").alias("is_morning"))
//        df2.show()
//
//        logins.createOrReplaceTempView("data")
//        val df3 = spark.sql("""
//        SELECT login_id, login_time,
//               CASE
//                   WHEN login_time < "12:00" THEN TRUE
//                   ELSE False
//               END AS Discount
//        FROM data
//    """)
//        println("output of sparksql")
//
//        df3.show()













  }
}
