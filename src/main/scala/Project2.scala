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

        // Senior citizen effect on  multiple factors--Complicated-- Top 10 Countries with aged population effect on covid rates-- RANKED
        val query1 = spark.sql("SELECT location country, MAX(median_age) median_age, MAX(aged_65_older+aged_70_older) senior_population, MAX(new_cases)/MAX(total_cases) * 100 infection_rate, MAX(people_fully_vaccinated)/MAX(Population) *100 vaccinated_population, MAX(new_deaths)/MAX(total_deaths) * 100 fatality_rate FROM covid_data GROUP BY location ORDER BY senior_population DESC")
        query1.createOrReplaceTempView("query1")
        spark.sql("Select *,RANK () OVER (ORDER BY infection_rate DESC) infection_rate_rank, RANK () OVER (ORDER BY vaccinated_population DESC) vaccinated_rank, RANK () OVER (ORDER BY fatality_rate DESC) fatality_rank from query1 ORDER BY senior_population DESC LIMIT 20").show()

        spark.sql("select location, gdp_per_capita, Max(total_deaths)/MAX(population)*100 as mortalityrate from covid_data group by location, gdp_per_capita order by gdp_per_capita Desc limit 10").show()

        // the death rates should be inversely proportionate to GDP but the data doesn’t show this. We can only assume that more things effect deaths than money

        spark.sql("SELECT MAX((total_deaths/population))*100 as mortality_Rate, location FROM covid_data group by location order by mortality_Rate Desc").show()

        spark.sql("SELECT location, hospital_beds_per_thousand, MAX((total_deaths/population)) mortalityrate FROM covid_data GROUP BY location, hospital_beds_per_thousand ORDER BY mortalityrate DESC").show()

        // Working on Amina's megaquery
        // First partition by location, then rank by date.
    }

}