package sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.nio.file.Files
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object RtmpLog {

  System.setProperty("com.amazonaws.services.s3.enableV4", "true")

  //Define a udf to concatenate two passed in string values
  val getConcatenated = udf((first: String, second: String) => { first + " " + second })

  def main(args: Array[String]) {

    val awsId = "accessKey"
    val awsKey = "secretKey"
    val url = "s3://bucketName/*.gz"

    val customSchema = StructType(Array(
      StructField("date", DateType, true),
      StructField("time", StringType, true),
      StructField("location", StringType, true),
      StructField("requestip", StringType, true),
      StructField("event", StringType, true),
      StructField("bytes", LongType, true),
      StructField("status", StringType, true),
      StructField("client_id", StringType, true),
      StructField("uri", StringType, true),
      StructField("uriquery", StringType, true),
      StructField("referrer", StringType, true),
      StructField("pageurl", StringType, true),
      StructField("useragent", StringType, true),
      StructField("sname", StringType, true),
      StructField("snamequery", StringType, true),
      StructField("fileext", StringType, true),
      StructField("sid", StringType, true)))

    //System.setProperty("hadoop.home.dir", "/hadoop/");

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("cloudfront-log-analysis")
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.warehouse.dir", "/data/spark/work")
      .config("fs.s3a.access.key", awsId)
      .config("fs.s3a.secret.key", awsKey)
      .config("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
      //.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      //.config("com.amazonaws.services.s3.enableV4","true")
      //.config("fs.s3a.impl.disable.cache", "false")
      //.config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
      //.config("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      //.config("fs.s3n.awsAccessKeyId", awsId)
      //.config("fs.s3n.awsSecretAccessKey", awsKey)
      .getOrCreate()

    val df = spark.read.option("delimiter", "\t")
      .option("header", "false")
      .option("comment", "#")
      .schema(customSchema)
      .csv(url)

    //utc+09 change
    val df2 = df.withColumn("dt", getConcatenated(col("date"), col("time")))
      .withColumn("kor_dt", from_utc_timestamp(col("dt"), "Asia/Seoul"))
      .select(col("dt"), col("kor_dt"), col("hostheader").as("cname"), col("bytes"))

    //domain 5min unit grouping
    val df3 = df2.groupBy(window(unix_timestamp(col("kor_dt")).cast("timestamp"), "5 minutes"), col("cname")).agg(sum(col("bytes")).as("bandwidth"))

    val resultDS = df3.withColumn("datetime", col("window").getField("start"))
      .withColumn("axis", to_date(col("window").getField("start")).cast("String"))
      .withColumn("axis_hour", hour(col("window").getField("start")))
      .withColumn("axis_minute", minute(col("window").getField("start")))
      .drop("window")
    resultDS.printSchema()
    resultDS.show(100, false)

    spark.stop()
  }
}