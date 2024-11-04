import org.apache.spark.{SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sales {
  def main(args:Array[String]):Unit= {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.master", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val sales = List(
      ("karthik", 60000),
      ("neha", 48000),
      ("priya", 30000),
      ("mohan", 24000),
      ("ajay", 52000),
      ("vijay", 45000),
      ("veer", 70000),
      ("aatish", 23000),
      ("animesh", 15000),
      ("nishad", 8000),
      ("varun", 29000),
      ("aadil", 32000)).toDF("name", "total_sales")

    sales.show()

    val performance_status_df = sales.withColumn("Performance_status" , when(col("total_Sales") > 50000 , "Excellent")
      .when(col("total_sales") < 50000 && col("total_sales") > 25000, "Good").otherwise("Needs Improvement").alias("Performance_status")).show()

    val agg_df = performance_status_df
      .groupBy("Performance_status")
      .agg(sum("total_sales").alias("total_sales"))
      .withColumn("name", initcap(col("name")))


  }
}