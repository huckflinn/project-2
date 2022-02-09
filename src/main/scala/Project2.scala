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
        val df = spark.read.options(Map("inferSchema" -> "true", "header" -> "true")).csv("input/covid-data.csv")        
        df.show()
    }

}