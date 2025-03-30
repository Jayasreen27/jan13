import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
object fourth {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.set("spark.app.name","spark-program")
    conf.set("spark.master","local[*]")



    val spark=SparkSession.builder()
      .config(conf)
      .getOrCreate()



    val  df=spark.read
      .format("csv")
      .option("header","true")
      .option("path","/Users/kalky/Documents/file operators folder/info.csv")
      .load()



    df.show()
  }

}
