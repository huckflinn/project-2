import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager

object Project2 {

    def main(args: Array[String]): Unit = {
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.1/libexec")

        val spark = SparkSession.builder().appName("Project2").config("spark.master", "local").config("spark.eventLog.enabled", "false").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        insertCovidData(spark)

        spark.stop()
    }

    def insertCovidData(spark: SparkSession): Unit = {
        val df = spark.read.options(Map("inferSchema" -> "true", "header" -> "true")).csv("input/covid-data_date_formatted.csv")
        df.createOrReplaceTempView("covid_data")

        spark.sql("SELECT location, date, population, people_fully_vaccinated, people_fully_vaccinated/population percent_fully_vaccinated, new_cases, new_cases/population new_case_ratio FROM covid_data WHERE date = '02/01/22' ORDER BY percent_fully_vaccinated DESC LIMIT 20").show()

        spark.sql("SELECT location, date, population, people_fully_vaccinated, people_fully_vaccinated/population percent_fully_vaccinated, new_cases, new_cases/population new_case_ratio FROM covid_data WHERE (date = '02/01/22') AND (people_fully_vaccinated IS NOT NULL) ORDER BY percent_fully_vaccinated ASC LIMIT 20").show()

        spark.sql("SELECT DISTINCT location, hospital_beds_per_thousand, life_expectancy FROM covid_data ORDER BY hospital_beds_per_thousand DESC LIMIT 20").show()
    }

}