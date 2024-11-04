import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,initcap, when,to_date}
import java.time.LocalDate
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object hello {
  def main(args:Array[String]):Unit= {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.master", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val employees = List(
      ("karthik", "2024-11-01"),
      ("neha", "2024-10-20"),
      ("priya", "2024-10-28"),
      ("mohan", "2024-11-02"),
      ("ajay", "2024-09-15"),
      ("vijay", "2024-10-30"),
      ("veer", "2024-10-25"),
      ("aatish", "2024-10-10"),
      ("animesh", "2024-10-15"),
      ("nishad", "2024-11-01"),
      ("varun", "2024-10-05"),
      ("aadil", "2024-09-30")
    ).toDF("name", "last_checkin")

    employees.show()
    val updated_df = employees.withColumn("last_checkin", to_date(col("last_checkin"), "yyyy-MM-dd"))

    val current_date =  LocalDate.now()
    val lastCheckin_date = java.sql.Date.valueOf(current_date.minusDays(7))


    val result_df = updated_df.withColumn("status",when(col("last_checkin") >= lastCheckin_date, "Active").otherwise("Inactive")).withColumn("name", initcap(col("name")))

    result_df.show()




  }}





