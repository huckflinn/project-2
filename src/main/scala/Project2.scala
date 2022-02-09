// import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.util.Scanner
import java.sql.DriverManager
import java.sql.Statement
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame

object Project2 {

    def main(args: Array[String]): Unit = {
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.1/libexec")

        val spark = SparkSession.builder().appName("SparkHelloWorld").config("spark.master", "local").config("spark.eventLog.enabled", "false").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        insertCovidData(spark)
    }

    def insertCovidData(spark: SparkSession): Unit = {
        // val output = sc.read.format("csv").option("inferSchema", "true").option("header", "true").load("input/covid-data.csv")
        val df = spark.read.text("input/covid-data.csv")
        df.withColumn("")
        df.limit(5).show()

        

        df.select("*")

        // spark.sql("CREATE TABLE IF NOT EXISTS covid_data (iso_code STRING, continent STRING, location STRING, date STRING, total_cases LONG, new_cases LONG, total_deaths LONG, new_deaths LONG, new_tests LONG, total_tests LONG, total_vaccinations LONG, people_vaccinated LONG, people_fully_vacccinated LONG, population LONG, population_density FLOAT, median_age FLOAT, aged_65_older FLOAT, aged_70_older FLOAT, gdp_per_capita FLOAT, hospital_beds_per_thousand FLOAT, life_expectancy FLOAT)")

        // spark.sql("INSERT INTO covid_data SELECT * FROM temp_data")

        
        df.show()

    }

}