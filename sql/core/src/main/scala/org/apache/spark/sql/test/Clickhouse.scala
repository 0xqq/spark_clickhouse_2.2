package org.apache.spark.sql.test

import org.apache.spark.sql.{Row, SparkSession}
import java.util.Properties

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row


/**
  * Created by admin on 18/4/8.
  */
object Clickhouse {

  def main(args: Array[String]) {

    //val blockSize = if (args.length > 2) args(2) else "4096"

    val spark = SparkSession
      .builder()
      .appName("Broadcast Test")
      .getOrCreate()


    val connectionProperties = new Properties()
    connectionProperties.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")
    connectionProperties.put("clickhouseIp", "172.22.16.38,172.22.16.39,172.22.16.41")
    connectionProperties.put("clickhouseJdbcPort", "8124")
    connectionProperties.put("clickhouseDb", "zampda_local")


    val a = spark.read.jdbc("jdbc:clickhouse://172.22.16.44:8124/zampda", "(select Age ,Gender from zampda_local.bid_local where  EventDate < '2018-04-04'  and EventDate > '2018-04-02' and Time_Hour < 2 ) t1", connectionProperties,true)
      .repartition(10)


    val schema = StructType(Seq(
      StructField("Age", StringType),
      StructField("Gender", StringType)
    ))

    val encoder = RowEncoder(schema)

    a.take(10).foreach( v => println(v))

    spark.stop()
    //println( )
    //val b =   a.map { case Row(age :String, gender :String)  => Row(age) }(encoder)
    //b.take(10)
  }

}
