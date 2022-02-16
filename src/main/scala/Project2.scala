import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame // Windows users must comment this line out
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.util.Scanner
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object Project2 {

    def main(args: Array[String]): Unit = {
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.1/libexec")  // for Mac
        // System.setProperty("hadoop.home.dir", "C:\\hadoop\\")  // for Windows
        val spark = SparkSession.builder().appName("Project2").config("spark.master", "local").config("spark.eventLog.enabled", "false").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        var scanner =new Scanner(System.in)
        scanner.useDelimiter(System.lineSeparator())
        var validChoice = false
        var end = false

        println(Console.WHITE)
        println(Console.BLACK_B)
        while (end == false) {
            println("")
            println("")
            println(Console.MAGENTA)
            println("Hello, Welcome to the GLOBAL COVID-19 DATABASE, LAST UPDATED : Feb 10 2022")
            println(Console.WHITE)
            println("It is currently: " + DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now))
            println("")
            println("")
            validChoice = false
            val df = spark.read.options(Map("inferSchema" -> "true", "header" -> "true")).csv("/project-2/covid-data_date_formatted.csv")
            df.createOrReplaceTempView("covid_data")
            while(validChoice == false) {
                println(Console.MAGENTA)
                println("Select one of the options below: ")
                println(Console.WHITE)
                println("1.) What countries have highest amount of vaccinations?")
                println("2.) What countries have lowest amount of vaccinations? ")
                println("3.) What has been the deadliest days since Covid began?")
                println("4.) What countries have the worst death rates?")
                println("5.) How does the total amount of hospital beds affect mortality rates")
                println("6.) Does having a higher GDP mean more hospital beds/better infrastructure?")
                println("7.) How does GDP effect the mortality rate?")
                println("8.) How did countries with a higher senior population handle COVID?")
                println("9.) What countries had the highest spread rate of infections?")
                println("10.) Do countries more densely populated have high rates of infection spreading?")
                println("")
                println("0.) EXIT")
                var choice = scanner.next().toString()

                if (choice == "1") {
                // percent of population vaccinated DESC
		            spark.sql("SELECT location country, population, MAX(people_fully_vaccinated) people_fully_vaxxed, ROUND(MAX(people_fully_vaccinated)/population, 2) percent_fully_vaxxed FROM covid_data WHERE people_fully_vaccinated IS NOT NULL GROUP BY country, population ORDER BY percent_fully_vaxxed DESC LIMIT 20").show()
                    validChoice = true

                } else if (choice == "2") {
                // percent of population vaccinated ASC
                    spark.sql("SELECT location country, population, MAX(people_fully_vaccinated) people_fully_vaxxed, ROUND(MAX(people_fully_vaccinated)/population, 2) percent_fully_vaxxed FROM covid_data WHERE (people_fully_vaccinated IS NOT NULL) AND (location NOT LIKE 'Northern Cyprus') GROUP BY country, population ORDER BY percent_fully_vaxxed ASC LIMIT 20").show()
                    validChoice = true

                } else if (choice == "3") {
                // Deadliest Days -- top 10 deadliest days globally and how many cases 
                    val query3 = spark.sql("Select date deadly_dates, SUM(new_deaths) Total_new_deaths, SUM(total_deaths) Total_Deaths from covid_data GROUP BY date ORDER BY SUM(new_deaths) DESC")
                    query3.createOrReplaceTempView("deadly_date")
                    spark.sql("Select deadly_dates, Total_new_deaths, Total_Deaths, RANK () OVER (ORDER BY Total_new_deaths DESC) deadly_ranked from deadly_date ORDER BY Total_new_deaths DESC LIMIT 20").show()
                    validChoice = true

                } else if (choice == "4") {
                // location vs deathrate
                    spark.sql("SELECT MAX((total_deaths/population))*100 as mortality_Rate, location country FROM covid_data group by location order by mortality_Rate Desc").show()
                    validChoice = true  
  
                } else if (choice == "5") {
                // hospital beds vs mortality rate
                    spark.sql("SELECT location country, hospital_beds_per_thousand, MAX((total_deaths/population)) * 100 mortalityrate FROM covid_data GROUP BY location, hospital_beds_per_thousand ORDER BY mortalityrate DESC").show()
                    validChoice = true  
  
                } else if (choice == "6") {
                // hospital beds vs GDP
                    spark.sql("SELECT DISTINCT location country, hospital_beds_per_thousand * 1000 beds, gdp_per_capita GDP FROM covid_data WHERE (hospital_beds_per_thousand IS NOT NULL) AND (gdp_per_capita IS NOT NULL) ORDER BY GDP DESC LIMIT 20").show()
                    validChoice = true    

                } else if (choice == "7") {
                //  GDP vs mortality rate
                    spark.sql("select location country, gdp_per_capita, Max(total_deaths)/MAX(population)*100 as mortalityrate from covid_data group by location, gdp_per_capita order by gdp_per_capita Desc limit 10").show()
                    validChoice = true
   
                } else if (choice == "8") {
                // senior population comparison
                    val query8 = spark.sql("SELECT location country, MAX(median_age) median_age, MAX(aged_65_older+aged_70_older) senior_population, AVG(new_cases)/AVG(total_cases) * 100 spread_rate, MAX(people_fully_vaccinated)/MAX(Population) * 100 vaccinated_population, AVG(new_deaths)/AVG(total_deaths) * 100 fatality_rate FROM covid_data GROUP BY location ORDER BY senior_population DESC")
                    query8.createOrReplaceTempView("aged_population")
                    spark.sql("Select *,RANK () OVER (ORDER BY spread_rate ASC) spread_rate_rank, RANK () OVER (ORDER BY vaccinated_population DESC) vaccinated_rank, RANK () OVER (ORDER BY fatality_rate DESC) fatality_rank from aged_population ORDER BY senior_population DESC LIMIT 20").show()
                    validChoice = true

                } else if (choice == "9") {
                // spread rate using multi table -- top 10 countries by avg spread rate per capita
                    val query9 = spark.sql("Select location country, population total_population, population_density population_density, new_cases/total_cases spread_rate from covid_data")
                    query9.createOrReplaceTempView("spread_rate")
                    spark.sql("Select country, first_value(total_population) total_population, COALESCE(AVG(spread_rate)) * 100 avg_spread_rate from spread_rate WHERE country NOT LIKE '%income%' AND country NOT LIKE '%Europe%' AND country NOT LIKE '%America%' AND country NOT LIKE 'Africa' Group BY country ORDER BY avg_spread_rate DESC").show()
                    validChoice = true

                } else if (choice == "10") {
                // population density to spread rate comparision using previous table
                    val query10 = spark.sql("Select location country, population total_population, population_density population_density, new_cases/total_cases spread_rate from covid_data")
                    query10.createOrReplaceTempView("spread_density")
                    val subquery10 = spark.sql("Select country, first_value(total_population) total_population, MAX(population_density) population_density, COALESCE(AVG(spread_rate)) * 100 avg_spread_rate from spread_density Group BY country ORDER BY population_density DESC")
                    subquery10.createOrReplaceTempView("ranked_spread")
                    spark.sql("SELECT country, total_population, population_density, avg_spread_rate, RANK () OVER (ORDER BY avg_spread_rate DESC) spread_ranked from ranked_spread WHERE country NOT LIKE 'Nauru' AND country NOT LIKE '%Sint%'  ORDER BY population_density DESC").show()
                    validChoice = true

                } else if (choice == "0") {
                    println("Spread Love Not Germs <3. Goodbye!")
                         println(Console.RED+"    .....           .....")
                     println(Console.RED+",ad8PPPP88b,     ,d88PPPP8ba,")
                   println(Console.RED+"d8P\"      \"Y8b, ,d8P\"      \"Y8b")
                    println(Console.RED+"dP'           \"8a8\"           `Yd")
                    println(Console.RED+"8              \"              )8")
                    println(Console.RED+"I8                             8I")
                    println(Console.RED+"Yb,                         ,dP")
                    println(Console.RED+"\"8a,                     ,a8\"")
                        println(Console.RED+"\"8a,                 ,a8\"")
                        println(Console.RED+"\"Yba             adP\"")   
                            println(Console.RED+"`Y8a         a8P\'")
                            println(Console.RED+"`88,     ,88\'")
                              println(Console.RED+"\"8b   d8\"")
                                println(Console.RED+"\"8b d8\"")
                                println(Console.RED+"`888'")
                                  println(Console.RED+"\"")

                validChoice = true
                end = true    
                } else {
                    println("Invalid Entry.... Choose from available options.")
                }
            }
        }

        spark.stop()
        scanner.close
    }
}