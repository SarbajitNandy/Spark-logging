package sarbajit

import org.apache.spark.sql.SparkSession
import org.apache.log4j.PatternLayout
import org.apache.log4j.{Level, Logger, FileAppender}

object Spark2 {
  val fa = new FileAppender
  fa.setName("FileLogger")
  fa.setFile("src/tmp/log4j/out.log")
  fa.setLayout(new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"))
  fa.setThreshold(Level.DEBUG)
  fa.setAppend(true)
  fa.activateOptions

  //add appender to any Logger (here is root)
  Logger.getRootLogger.addAppender(fa)

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("My Logger")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("AtomicEngineering")
      .getOrCreate()

    logger.info("Spark application started")
    println("Printing Spark Session Variables:")
    println("App Name: " + spark.sparkContext.appName)
    println("Deployment Mode: " + spark.sparkContext.deployMode)
    println("Master: " + spark.sparkContext.master)

    spark.stop()
    logger.info("Spark application stopped")
  }
}