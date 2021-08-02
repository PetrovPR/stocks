package com.ppetrov

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, functions}


object StocksService {


  def init(spark: SparkSession): Unit = {

    ////////////////////////////////////////2  Create Spark RDD from the data sets for AMZN and APPL stock prices
    val amazonRdd: RDD[Row] = getCvsData(
      "./src/main/resources/AMZNtest.csv",
      true,
      spark
    )

    val appleRdd: RDD[Row] = getCvsData(
      "./src/main/resources/HistoricalQuotes.csv",
      true,
      spark
    )

    //////////////////////////////////////3  Write 2+ exploratory SQL queries for each dataset

    val amazonDF = spark.createDataFrame(amazonRdd, getAmazonSchema()).toDF()

    val appleDF = spark.createDataFrame(appleRdd, getAppleSchema()).toDF()

    amazonDF.createTempView("amazon")
    appleDF.createTempView("apple")


    val allAmazonValues = spark.sql("Select * from amazon")

    val selectTop5DiffMaxAndMinPrice = spark.sql("Select date, (DOUBLE(open_amazon) -DOUBLE(closed_amazon)) as diff from amazon order by diff desc limit 5")
    // avr by month
    spark.sql("WITH tmp AS " +
      "(Select month(to_date(date, 'MM/dd/yyyy')) as month, " +
      "year(to_date(date, 'MM/dd/yyyy')) as year, " +
      "(double(substring(trim(closed_apple), 2))) as price from apple order) " +
      "Select max(tmp.price), first(tmp.month), first(year) from tmp " +
      "group by (tmp.month, tmp.year)")

    val listOfAppleClosed = spark.sql("Select date, (substring(trim(closed_apple), 2)) from apple")

    val diffBetweenMaxAndClosed = spark.sql("Select to_date(date, 'yyyy-MM-dd') as date, (DOUBLE(open_amazon) -DOUBLE(closed_amazon)) as diff from amazon order")

    amazonDF.filter(amazonDF("open_amazon") > "1530").select("open_amazon", "high_amazon", "low_amazon")

    //GET avg volume
    amazonDF.agg(avg(amazonDF("volume_amazon")))

    //when was the max price on close
    amazonDF.filter(amazonDF("closed_amazon") === amazonDF
      .agg(functions.max(amazonDF("closed_amazon"))).
      collect()(0)
      .getString(0))

    /// Transform dataframes to be on the same time scale and compare stock prices on a daily, weekly, monthly basis
    val amazonDFWithScaledate = amazonDF.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

    /////////////////////////////////////////  compare stocks daily
    val appleDFWithScaleDate = appleDF.columns
      .foldLeft(appleDF)(
        (acc, column) => acc
          .withColumn(column, regexp_replace(col(column), "\\$", "")))
      .withColumn("date", to_date(col("date"), "MM/dd/yyyy"))

    val dailyStocksDF = appleDFWithScaleDate.join(amazonDFWithScaledate, amazonDFWithScaledate("date") === appleDFWithScaleDate("date"))
      .drop(amazonDFWithScaledate("date"))


    ////////////////////Get DF amazon stock to the end of the every weeks
    val appleAvrByWeeksDF = spark.sql("WITH tmp AS " +
      "(Select concat(string(year(to_date(date, 'MM/dd/yyyy'))),'-',string(month(to_date(date, 'MM/dd/yyyy'))),'-',string(weekofyear(to_date(date, 'MM/dd/yyyy')))) " +
      "as year_month_week, (double(substring(trim(closed_apple), 2))) as price, date from apple)" +
      " Select avg(price) as apple_price, first(year_month_week) as date from tmp group by year_month_week")

    val amazonAvrByWeeksDF = spark.sql(
      "WITH tmp AS " +
        "(Select concat(string(year(to_date(date, 'yyyy-MM-dd'))),'-',string(month(to_date(date, 'yyyy-MM-dd')))" +
        ",'-',string(weekofyear(to_date(date, 'yyyy-MM-dd')))) as year_month_week, " +
        "double(closed_amazon) as price, date from amazon) " +
        "Select avg(price) as amazon_price, first(year_month_week) as date from tmp group by year_month_week")

    amazonAvrByWeeksDF.join(appleAvrByWeeksDF, appleAvrByWeeksDF("date") === amazonAvrByWeeksDF("date"))
      .drop(amazonAvrByWeeksDF("date"))

    /////////////////////  Compare By month
    val appleDFByLastDatyOfMonth = appleDFWithScaleDate
      .withColumn("last_date_of_month", last_day(col("date")))
      .filter("last_date_of_month == date")

    val amazonDFByLastDatyOfMonth = amazonDFWithScaledate
      .withColumn("last_date_of_month", last_day(col("date")))
      .filter("last_date_of_month == date")

    val comparedDFByMonth = appleDFByLastDatyOfMonth
      .join(amazonDFByLastDatyOfMonth, amazonDFByLastDatyOfMonth("date") === appleDFByLastDatyOfMonth("date"))
      .drop(amazonDFByLastDatyOfMonth("date"))
      .drop("last_date_of_month")

    ////////////////////////------------Write UDF to mark on daily bases which stock was higher------------//////////
    val convertStringToInt = udf(
      (applePrise: String, amazonPrise: String) => {
        if (applePrise.toDouble > amazonPrise.toDouble) "Apple is higher"
        else "Amazon is higher"
      }
    )

    val whoIsHigherStocks =  dailyStocksDF.withColumn("who_is_higher", convertStringToInt(col("closed_apple"), col("closed_amazon")))


    ////////////////////----------------------Use the dataframe from step 5 to save in parquet format
    whoIsHigherStocks.write.partitionBy("who_is_higher").parquet("stocks.parquet")
  }

  /**
   * Getting data from csv knowing host and header
   *
   * @param host          url of the csv file
   * @param isHeaderExist set true if the header in the scv file exist
   * @param spark         instance of SparkSession
   * @return new instance of RDD file
   */
  private def getCvsData(host: String, isHeaderExist: Boolean, spark: SparkSession): RDD[Row] = {
    spark.sparkContext.addFile(host)
    spark
      .read.format("csv")
      .option("header", isHeaderExist.toString)
      .load(host).rdd
  }

  /**
   * Get instance of  Amazon DF schema
   *
   * @return new Structured type as new amazon DF schema
   */
  private def getAmazonSchema() = {
    new StructType()
      .add(StructField("date", StringType, false))
      .add(StructField("open_amazon", StringType, false))
      .add(StructField("high_amazon", StringType, false))
      .add(StructField("low_amazon", StringType, false))
      .add(StructField("closed_amazon", StringType, false))
      .add(StructField("adj_amazon", StringType, false))
      .add(StructField("volume_amazon", StringType, false))
  }

  /**
   * Get instance of  Apple DF schema
   *
   * @return new Structured type as new apple DF schema
   */
  private def getAppleSchema() = {
    new StructType()
      .add(StructField("date", StringType, false))
      .add(StructField("closed_apple", StringType, false))
      .add(StructField("volume_apple", StringType, false))
      .add(StructField("open_apple", StringType, false))
      .add(StructField("high_apple", StringType, false))
      .add(StructField("low_apple", StringType, false))
  }

}
