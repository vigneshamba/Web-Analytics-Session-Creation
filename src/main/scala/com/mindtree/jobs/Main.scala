package com.mindtree.jobs

import com.mindtree.constants.Constants.JobName
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


object Main extends App {
  val logger = LoggerFactory.getLogger(JobName)
  val spark = SparkSession.builder().appName(JobName).master("local")
    //.enableHiveSupport()
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()

  val InputHivePath = "D:\\frameworks\\Web-Analytics-Data-Ingestion\\data";
  val sessionTimeOut=getSessionTimeOut
  val job = new UserSessionCreationJob(spark, InputHivePath,sessionTimeOut)
  job.process()
def getSessionTimeOut() =
  {
    if(!args.isEmpty)
     args(0).toInt
    else
      300
  }
}
