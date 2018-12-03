package com.mindtree.jobs

import com.mindtree.constants.Constants._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

class UserSessionCreationJob(spark: SparkSession, inputHivePath: String, sessionTimeOut: Int) extends Serializable {

  def process(): Unit = {

    val records = spark.read.parquet(inputHivePath)//local
    //val records = spark.read.table(Tier1TableName)

    val transformedDF = records.withColumn(TimestampColName, unix_timestamp(records(TimestampColName), TimestampFormat).cast(TimestampType))
    val userWindow = Window.partitionBy(MaskedHostColName).orderBy(TimestampColName)
    val userSessionWindow = Window.partitionBy(MaskedHostColName, SessionIdColName)


    val newSession = (coalesce(
      unix_timestamp(transformedDF(TimestampColName)) - unix_timestamp(lag(transformedDF(TimestampColName), 1).over(userWindow)),
      lit(0)
    ) > sessionTimeOut).cast("bigint")

    val dfWithSession = transformedDF.withColumn(SessionIdColName, sum(newSession).over(userWindow))

    dfWithSession.show(false)
    val dfWithStartTime = dfWithSession
      .withColumn(SessionStartTimeColName, min(dfWithSession.col(TimestampColName)).over(userSessionWindow))


    val dfWithEndTime = dfWithStartTime
      .withColumn(SessionEndTimeColName, max(dfWithStartTime.col(TimestampColName)).over(userSessionWindow))

    val dfWithFirstPage = dfWithEndTime.
      withColumn(FirstPageColName, first(dfWithEndTime.col(URIColName)).over(userSessionWindow))

    val dfWithLastPage = dfWithFirstPage.
      withColumn(LastPageColName, last(dfWithFirstPage.col(URIColName)).over(userSessionWindow))

    val dfWithUniqueSessionKey = dfWithLastPage.
      withColumn(SessionIdColName, sha1(concat(dfWithLastPage.col(MaskedHostColName), dfWithLastPage.col(SessionIdColName))))

    val result = dfWithUniqueSessionKey.groupBy(SessionIdColName, MaskedHostColName,
      SessionStartTimeColName, SessionEndTimeColName, FirstPageColName, LastPageColName).count().alias(TotalRequests)
      .select(SessionIdColName, MaskedHostColName, SessionStartTimeColName, SessionEndTimeColName,
        FirstPageColName, LastPageColName, TotalRequests)

    //result.write.insertInto(SessionTable)
    result.write.mode("append").format("parquet").save("data")
  }

}
