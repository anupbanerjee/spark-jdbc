package com.anup.sample

import java.util.Properties

import org.apache.spark.sql.SparkSession

object PostgresDataImport {

  case class Person(name: String, age: Long)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example").master("local[4]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    runJdbcDatasetExample(spark)
    spark.stop()
  }

  private def runJdbcDatasetExample(spark: SparkSession): Unit = {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "cmts_ipdr_data")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    //jdbcDF.printSchema()
    //jdbcDF.select("cmts_id").show()
    //jdbcDF.show()

    //val jdbcDF2 = jdbcDF.groupBy("cmts_id", "ipaddress").sum("upstream", "downstream").orderBy("cmts_id", "ipaddress")
    val jdbcDF2 = jdbcDF.groupBy("cmts_id").sum("upstream","downstream").orderBy("cmts_id")

    jdbcDF2.show(100000)

    println(jdbcDF.count())

    //jdbcDF.groupBy("cmts_id", "ipaddress").count().show(100000)
    

    /*
    val connectionProperties = new Properties()
    connectionProperties.put("user", "anup.banerjee")
    connectionProperties.put("password", "anup.banerjee")
    
    //val jdbcDF2 = spark.read.jdbc("jdbc:postgresql://localhost:5432/cmtsdata", "cmts_ipdr_data", connectionProperties)

    // Saving data to a JDBC source
    jdbcDF.write
      .format("jdbc")
       .option("url", "jdbc:postgresql:cmtsdata")
      .option("dbtable", "cmts_ipdr_data_new")
      .option("user", "anup.banerjee")
      .option("password", "anup.banerjee")
      .save()

    jdbcDF2.write
      .jdbc("jdbc:postgresql:cmtsdata", "cmts_ipdr_data_new_1", connectionProperties)

    // Specifying create table column data types on write
    jdbcDF.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:cmtsdata", "cmts_ipdr_data_new_1", connectionProperties)
    // $example off:jdbc_dataset$
*/ }

}