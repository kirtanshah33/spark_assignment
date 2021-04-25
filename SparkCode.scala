package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit, split, unix_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}

object SparkCode extends App {
  val input = args(0)
  val output = args(1)
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("spark-app")
    .getOrCreate();
  val schema = StructType(
    List(
      StructField("message", StringType, nullable = true),
    )
  )

  val inputFile = spark.readStream.schema(schema).json(input)
  val filteredMessage = inputFile.filter(col("message").contains("omwssu"))
  val parsedMessage = filteredMessage.withColumn("fields", split(col("message"), " +"))

  val extractFields = parsedMessage.select(col("fields").getItem(12).as("url"),
    concat_ws(" ",col("fields").getItem(1), col("fields").getItem(2)).as("timestamp_string"))

  val convertTimestamp = extractFields.withColumn("timestamp", unix_timestamp(col("timestamp_string")).
    cast(TimestampType)).drop("timestamp_string")

  val splitUrl = convertTimestamp.withColumn("url_fields",split(col("url"),"/"))

  val fqdn_field = concat_ws("/", concat(col("url_fields").getItem(0),lit("/")),
    col("url_fields").getItem(2),
    col("url_fields").getItem(3))
  val cpe_id_field = col("url_fields").getItem(4)
  val action_field = col("url_fields").getItem(5)
  val error_code_1 = col("url_fields").getItem(6)
  val error_code_2 = col("url_fields").getItem(7)
  val error_code = concat_ws(".",error_code_1,error_code_2).cast(DoubleType)

  val parseUrl = splitUrl.withColumn("fqdn",fqdn_field).
    withColumn("cpe_id",cpe_id_field).
    withColumn("error_code",error_code).drop(col("url")).drop(col("url_fields"))

  parseUrl.writeStream.format("json").option("checkpointLocation","\\home\\checkpoints\\").option("path",output).
    outputMode("append").trigger(Trigger.ProcessingTime("10 seconds")).
    start().awaitTermination()
}
